import time
import random
import copy
import requests
from merkle_tree import MerkleTree
from vector_clock_utils import compare_clocks
from terminal_colors import TC

# --- Hinted Handoff Worker ---
def hint_handoff_worker(hint_manager, node_states, gossip_state_lock, shutdown_event, http_session):
    """Periodically checks for hints and tries to deliver them."""
    while True:
        # Wait for the interval, but exit early if shutdown is signaled
        if shutdown_event.wait(timeout=20):
            print(TC.colorize("Hint Handoff Worker: Shutdown signaled, exiting.", TC.YELLOW))
            break
        
        hinted_nodes = hint_manager.get_all_hinted_nodes()
        if not hinted_nodes:
            continue

        print(TC.colorize(f"Hint Handoff: Found hints for nodes: {hinted_nodes}", TC.LIGHT_MAGENTA))
        for node_id in hinted_nodes:
            try:
                # Check our local gossip state to see if the node is considered 'up'
                with gossip_state_lock:
                    is_node_up = node_states.get(node_id, {}).get('status') == 'up'
                if is_node_up:
                    print(TC.colorize(f"Hint Handoff: Node {node_id} appears to be online. Attempting to deliver hints...", TC.LIGHT_MAGENTA))
                    hints_to_deliver = hint_manager.get_and_clear_hints(node_id)
                    if not hints_to_deliver: continue
                    
                    failed_hints = []
                    for hint in hints_to_deliver:
                        try:
                            # The hint object now correctly contains the vector clock. Use HTTPS.
                            payload = {
                                'key': hint['key'], 
                                'value': hint['value'], 
                                'vector_clock': hint.get('vector_clock', {}),
                                'source': 'hinted-handoff' # Add the source marker
                            }
                            # Send the original POST request to the recovered node
                            handoff_res = http_session.post(f"https://{node_id}/internal/kv", json=payload, timeout=5)
                            handoff_res.raise_for_status()
                            print(TC.colorize(f"Hint Handoff: Successfully delivered hint for key '{hint['key']}' to {node_id}.", TC.MAGENTA))
                        except requests.exceptions.HTTPError as e:
                            # If we get a 409 Conflict, the target node already has a newer version. The hint is obsolete.
                            if e.response.status_code == 409:
                                print(TC.colorize(f"Hint Handoff: Hint for key '{hint['key']}' rejected by {node_id} (409 Conflict). Discarding as obsolete.", TC.YELLOW))
                            # For other HTTP errors, add the hint to the failed list to be re-stored.
                            else:
                                print(TC.colorize(f"Hint Handoff: HTTP error delivering hint {hint['key']} to {node_id}: {e}", TC.RED))
                                failed_hints.append(hint)
                        except requests.RequestException as e:
                            print(TC.colorize(f"Hint Handoff: Failed to deliver hint {hint['key']} to {node_id}: {e}", TC.RED))
                            # If delivery fails due to a network error, add the hint to the failed list.
                            failed_hints.append(hint)
                    
                    # If any hints failed, re-store them for a future attempt.
                    if failed_hints:
                        print(TC.colorize(f"Hint Handoff: Re-storing {len(failed_hints)} failed hints for node {node_id}.", TC.YELLOW))
                        for hint in failed_hints:
                            hint_manager.store_hint(node_id, hint['key'], hint['value'], hint['vector_clock'], source=hint.get('source'))
                    elif hints_to_deliver:
                        print(TC.colorize(f"Hint Handoff: Successfully delivered all hints to {node_id}.", TC.LIGHT_MAGENTA, bold=True))
                else:
                    print(TC.colorize(f"Hint Handoff: Target node {node_id} is not 'up' in local state. Will retry later.", TC.GRAY, dim=True))
            except Exception as e:
                print(TC.colorize(f"Error in hint handoff worker: {e}", TC.RED))

# --- Anti-Entropy Worker (Merkle Tree) ---
def anti_entropy_worker(kv_store, node_states, gossip_state_lock, shutdown_event, http_session, anti_entropy_cache):
    """
    Periodically builds a Merkle tree from local data to detect inconsistencies.
    """
    while True:
        if shutdown_event.wait(timeout=60):
            print(TC.colorize("Anti-Entropy Worker: Shutdown signaled, exiting.", TC.YELLOW))
            break

        with gossip_state_lock: # Use the same lock to access kv_store safely
            if shutdown_event.is_set(): continue # Don't start new work if shutting down
            # Get all data blocks from the local store
            # Copy the data quickly and release the lock before heavy computation
            all_data = [dict(item, key=key) for key, item in kv_store.kvstore.items()]

        # Perform CPU-intensive work outside the lock
        merkle_tree = MerkleTree(all_data)
        root_hash = merkle_tree.get_root_hash()
        anti_entropy_cache['merkle_tree'] = merkle_tree # Update the cache

        # Update our own merkle root in the in-memory state for gossiping
        with gossip_state_lock:
            node_states[kv_store.node_id]['merkle_root'] = root_hash

        mismatched_peers = []
        with gossip_state_lock:
            if shutdown_event.is_set(): continue
            
            # Find peers to sync with inside the lock
            for node_id, state in node_states.items():
                if node_id == kv_store.node_id or state.get('status') != 'up':
                    continue
                
                peer_root = state.get('merkle_root')
                # Trigger sync if roots are different, or if one has a root and the other doesn't.
                # This handles the startup case where a peer's root hash isn't known yet.
                if root_hash != peer_root:
                    # Don't call sync_with_peer here. Just make a to-do list.
                    mismatched_peers.append((node_id, peer_root))
        
        # Now, perform all slow network operations OUTSIDE the lock
        for node_id, peer_root in mismatched_peers:
            print(TC.colorize(f"\n--- Anti-Entropy: Mismatch detected with {node_id} ---", TC.CYAN))
            print(TC.colorize(f"  My Root:    {root_hash}", TC.CYAN))
            print(TC.colorize(f"  Peer Root:  {peer_root}", TC.CYAN))
            # Pass the lock in, but don't hold it during the initial call.
            # The sync_with_peer function will acquire it only when it needs to write to the kv_store.
            sync_with_peer(node_id, merkle_tree, root_hash, peer_root, kv_store, gossip_state_lock, http_session)


def sync_with_peer(peer_id, local_tree, local_root_hash, peer_root_hash, kv_store, gossip_state_lock, http_session):
    """Recursively traverses the Merkle trees to find and sync inconsistencies."""
    stack = [(local_root_hash, peer_root_hash)]

    while stack:
        my_hash, peer_hash = stack.pop()

        if my_hash == peer_hash:
            continue # This branch is in sync

        # Determine the children of each subtree. If a hash is None, it has no children.
        my_children = local_tree.get_children_hashes(my_hash) if my_hash else None
        peer_children = None
        if peer_hash:
            try:
                response = http_session.get(f"https://{peer_id}/anti-entropy/get-subtree-hashes/{peer_hash}", timeout=3)
                if response.status_code == 200:
                    peer_children = response.json()
                # If the peer returns 404, it means peer_hash is a leaf, so peer_children remains None.
                elif response.status_code != 404:
                    print(TC.colorize(f"Anti-Entropy: Failed to get subtree for {peer_hash} from {peer_id}. Status: {response.status_code}", TC.YELLOW))
                    continue
            except requests.RequestException as e:
                print(TC.colorize(f"Anti-Entropy: Error requesting subtree from {peer_id}: {e}", TC.RED))
                continue

        if my_children and peer_children:
            stack.append((my_children['left'], peer_children['left']))
            stack.append((my_children['right'], peer_children['right']))
        elif my_children and not peer_children:
            # We have a subtree, but the peer does not. Compare our children to None.
            stack.append((my_children['left'], None))
            stack.append((my_children['right'], None))
        elif not my_children and peer_children:
            # The peer has a subtree, but we do not. Compare its children to None.
            stack.append((None, peer_children['left']))
            stack.append((None, peer_children['right']))
        else:
            # Leaf mismatch. This block is reached if:
            # 1. Both my_hash and peer_hash are non-None leaves (my_children and peer_children are None).
            # 2. One hash is a non-None leaf and the other is None.
            # Prioritize asking the peer what key its hash belongs to. This is more robust.
            key_to_sync = None
            print(TC.colorize(f"Anti-Entropy: Leaf mismatch. Requesting key for hash {peer_hash} from peer {peer_id}.", TC.CYAN))
            try:
                key_res = http_session.get(f"https://{peer_id}/anti-entropy/get-key-for-hash/{peer_hash}", timeout=3)
                if key_res.status_code == 200:
                    key_to_sync = key_res.json().get('key')
            except requests.RequestException as e:
                print(TC.colorize(f"Anti-Entropy: Failed to get key for hash from {peer_id}: {e}", TC.RED))
                # Fallback to checking our own tree if the peer fails to respond.
                key_to_sync = local_tree.leaf_hashes.get(my_hash)

            if not key_to_sync: continue # If we can't determine the key, we can't sync.

            print(TC.colorize(f"Anti-Entropy: Syncing key '{key_to_sync}' from peer {peer_id}.", TC.LIGHT_CYAN))
            try:
                response = http_session.get(f"https://{peer_id}/anti-entropy/get-data-block/{key_to_sync}", timeout=3)
                if response.status_code == 200:
                    peer_data = response.json()
                    
                    # Acquire lock to perform an atomic read-compare-write operation.
                    # This prevents a race condition with concurrent client writes.
                    # This prevents race conditions with concurrent client writes.
                    with gossip_state_lock:
                        local_data = kv_store.get(key_to_sync)
                        
                        # --- Bidirectional Sync Logic ---
                        # Case 1: Peer has a newer version, or we don't have the key at all.
                        if not local_data or compare_clocks(peer_data['vector_clock'], local_data.get('vector_clock', {})) == 'descendant':
                            print(TC.colorize(f"Anti-Entropy: Updating local key '{key_to_sync}' with peer's version.", TC.CYAN, bold=True))
                            kv_store.put(key_to_sync, peer_data['value'], peer_data['vector_clock'])
                        
                        # Case 2: We have a newer version, and the peer is out of date.
                        elif local_data and compare_clocks(local_data['vector_clock'], peer_data.get('vector_clock', {})) == 'descendant':
                            print(TC.colorize(f"Anti-Entropy: Pushing local key '{key_to_sync}' to out-of-date peer {peer_id}.", TC.CYAN, bold=True))
                            try:
                                payload = {
                                    'key': key_to_sync,
                                    'value': local_data['value'],
                                    'vector_clock': local_data['vector_clock'],
                                    'source': 'anti-entropy'
                                }
                                http_session.post(f"https://{peer_id}/internal/kv", json=payload, timeout=5).raise_for_status()
                            except requests.RequestException as push_e:
                                print(TC.colorize(f"Anti-Entropy: Failed to push update for key '{key_to_sync}' to {peer_id}: {push_e}", TC.RED))

            except requests.RequestException as e:
                print(TC.colorize(f"Anti-Entropy: Failed to fetch data for key '{key_to_sync}' from {peer_id}: {e}", TC.RED))

# --- Gossip Protocol Worker ---
def gossip_worker(NODE_ID, node_states, gossip_state_lock, hash_ring, GOSSIP_INTERVAL, shutdown_event, http_session, fanout=3):
    """Periodically initiates gossip with a random peer."""
    while True:
        if shutdown_event.wait(timeout=GOSSIP_INTERVAL):
            print(TC.colorize("Gossip Worker: Shutdown signaled, exiting.", TC.YELLOW))
            break
        
        with gossip_state_lock:
            if shutdown_event.is_set(): continue
            # Increment our own version and update timestamp to show we are alive
            node_states[NODE_ID]['version'] += 1
            node_states[NODE_ID]['last_updated'] = time.time()
            
            suspect_nodes = []
            
            # Centralized Ring Management:
            # Iterate through all known nodes and sync the hash ring with the latest state.
            for node_id, state in node_states.items():
                is_suspect = False
                if node_id != NODE_ID:
                    time_since_update = time.time() - state['last_updated']
                    # If a node is 'down', only probe it occasionally to check for recovery.
                    # Here, we probe it every 6 gossip intervals.
                    if state['status'] == 'down' and time_since_update > GOSSIP_INTERVAL * 6:
                        # Only probe if we haven't already probed it recently.
                        if time.time() - state.get('last_probed', 0) > GOSSIP_INTERVAL * 6:
                            suspect_nodes.append(node_id)
                    # If a node is 'up' but we haven't heard from it in a while, it's suspect.
                    elif state['status'] == 'up' and time_since_update > GOSSIP_INTERVAL * 6:
                        suspect_nodes.append(node_id)

                # Sync hash ring based on current status
                node_in_ring = any(n.startswith(node_id) for n in hash_ring.get_all_physical_nodes())
                if state['status'] == 'up' and not node_in_ring:
                    print(TC.colorize(f"Gossip: Adding node {node_id} to the ring.", TC.GRAY, dim=True))
                    hash_ring.add_node(node_id)
                elif state['status'] == 'down' and node_in_ring:
                    print(TC.colorize(f"Gossip: Removing node {node_id} from the ring.", TC.GRAY, dim=True))
                    hash_ring.remove_node(node_id)

        # --- Perform network I/O outside the main state lock ---
        
        # Probe suspect nodes
        for node_id in suspect_nodes:
            print(TC.colorize(f"Gossip: Node {node_id} is suspect. Probing directly...", TC.YELLOW))
            try:
                http_session.get(f"https://{node_id}/health", timeout=2).raise_for_status()
                print(TC.colorize(f"Gossip: Probe of {node_id} successful. Node is alive.", TC.GREEN))
                with gossip_state_lock: # Re-acquire lock to update state
                    if node_id in node_states:
                        # If the node was marked down, mark it as up and increment version
                        if node_states[node_id]['status'] == 'down':
                            node_states[node_id]['status'] = 'up'
                            node_states[node_id]['version'] += 1
                        node_states[node_id]['last_updated'] = time.time()
            except requests.RequestException:
                with gossip_state_lock: # Re-acquire lock to update state
                    print(TC.colorize(f"Gossip: Probe of {node_id} failed. Marking as down.", TC.RED))
                    if node_id in node_states and node_states[node_id]['status'] == 'up':
                        node_states[node_id]['status'] = 'down'
                        node_states[node_id]['version'] += 1
                    # Record the time of this failed probe to prevent immediate re-probing.
                    node_states[node_id]['last_probed'] = time.time()

        # Prepare data for gossip AFTER all probing and potential state changes are complete.
        with gossip_state_lock:
            # Select peers from nodes that are 'up' and not currently in the suspect list we just probed.
            peers = [node_id for node_id, state in node_states.items() if node_id != NODE_ID and state['status'] == 'up' and node_id not in suspect_nodes]
            state_to_gossip = copy.deepcopy(node_states)

        if not peers:
            continue
        
        # Select multiple random peers to gossip with (fan-out)
        k = min(fanout, len(peers))
        target_peers = random.sample(peers, k)

        # Gossip with target peers
        for peer in target_peers:
            try:
                response = http_session.post(f"https://{peer}/gossip", json=state_to_gossip, timeout=2)
                handle_gossip_response(response.json(), node_states, gossip_state_lock)
            except requests.RequestException as e:
                print(TC.colorize(f"Gossip: Could not connect to peer {peer}: {e}", TC.GRAY, dim=True))

def handle_gossip_response(incoming_states, node_states, gossip_state_lock):
    """Helper to process gossip response without triggering a request loop."""
    with gossip_state_lock:
        for node_id, state in incoming_states.items():
            current_state = node_states.get(node_id)
            if not current_state or state['version'] > current_state['version']:
                node_states[node_id] = state