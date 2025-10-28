import os
import sys
import signal
import time
import requests
import random
import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from cryptography.fernet import Fernet
from flask import Flask, request, jsonify

from kv_store import KVStore
from Hash_Ring import HashRing
from hint_manager import HintManager
from vector_clock_utils import compare_clocks, merge_clocks, increment_clock
from node_workers import hint_handoff_worker, anti_entropy_worker, gossip_worker, handle_gossip_response
from terminal_colors import TC

GOSSIP_INTERVAL = 5  # seconds

# --- Helper to find local IP ---
def get_local_ip():
    """
    Attempts to determine the local IP address of the machine by connecting to an
    external address. Falls back to 127.0.0.1 if unable to determine.
    """
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Doesn't have to be a reachable address, it's just to find the outbound interface
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1' # Fallback
    finally:
        if s:
            s.close()
    return ip

# --- Configuration ---
# These should be passed as command-line arguments for flexibility
try:
    # Default values
    NODE_HOST = get_local_ip()
    NODE_PORT = 8001

    if len(sys.argv) > 1:
        # Case 1: python node_server.py <port>
        if sys.argv[1].isdigit():
            NODE_PORT = int(sys.argv[1])
        # Case 2: python node_server.py <host> <port>
        elif len(sys.argv) > 2 and sys.argv[2].isdigit():
            NODE_HOST = sys.argv[1]
            NODE_PORT = int(sys.argv[2])
        else:
            raise ValueError("Invalid arguments.")
except (IndexError, ValueError):
    print("Usage: python node_server.py [<port>] or python node_server.py <host> <port>")
    sys.exit(1)

NODE_ID = f"{NODE_HOST}:{NODE_PORT}"
# Create a dedicated directory for this node inside a parent 'data' folder
DATA_DIR = os.path.join('data', f'node_{NODE_PORT}')
os.makedirs(DATA_DIR, exist_ok=True)

# Use an environment variable for the gateway URL, with a sensible default.
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://localhost:8000")
ADMIN_TOKEN = "password" # Must match the gateway's token

# --- Quorum and Replication Configuration (now needed on the node) ---
REPLICATION_FACTOR = 3 # N
WRITE_QUORUM = 2       # W
READ_QUORUM = 1        # R


# --- SSL Configuration ---
# Define paths relative to the script's directory to ensure they are found.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CERT_FILE = os.path.join(SCRIPT_DIR, 'cert.pem')
KEY_FILE = os.path.join(SCRIPT_DIR, 'key.pem')

# --- Encryption at Rest Configuration ---
ENCRYPTION_KEY_FILE = 'secret.key'

def load_or_generate_encryption_key():
    """Loads the encryption key from a file, or generates a new one if it doesn't exist."""
    if os.path.exists(ENCRYPTION_KEY_FILE):
        with open(ENCRYPTION_KEY_FILE, 'rb') as f:
            return f.read()
    else:
        print(f"Generating new encryption key at: {ENCRYPTION_KEY_FILE}")
        key = Fernet.generate_key()
        with open(ENCRYPTION_KEY_FILE, 'wb') as f:
            f.write(key)
        return key

# --- Flask App and KVStore Initialization ---
app = Flask(__name__)

# --- Shutdown Event ---
shutdown_event = threading.Event()

# --- Gossip State and Local Hash Ring ---
gossip_state_lock = threading.RLock() # Use a Reentrant Lock for safety
# Format: { "node_id": {"version": int, "last_updated": float, "status": "up" | "down", "merkle_root": "hash"} }
node_states = {
    NODE_ID: {"version": 1, "last_updated": time.time(), "status": "up"}
}
# Each node now maintains its own hash ring, which is updated via gossip
hash_ring = HashRing(nodes=[NODE_ID])

# --- Anti-Entropy Cache ---
# The anti-entropy worker will update this, and endpoints will read from it.
anti_entropy_cache = {'merkle_tree': None}

# --- Initialize Components with Encryption ---
encryption_key = load_or_generate_encryption_key()
cipher = Fernet(encryption_key)

kv_store = KVStore(data_dir=DATA_DIR, node_id=NODE_ID, cipher=cipher)
hint_manager = HintManager(data_dir=DATA_DIR, cipher=cipher)

# --- Configured Requests Session ---
# Create a reusable session object to configure SSL settings for all outgoing requests.
http_session = requests.Session()
http_session.verify = False # For self-signed certs
http_session.cert = (CERT_FILE, KEY_FILE) # Use client-side cert for mTLS

# Thread pool for parallel requests to other nodes
executor = ThreadPoolExecutor(max_workers=10)

# --- Helper Functions ---

def register_with_gateway():
    """Registers this node with the central gateway."""
    url = f"{GATEWAY_URL}/register_node"
    headers = {'X-Admin-Token': ADMIN_TOKEN, 'Content-Type': 'application/json'}
    payload = {'node_id': NODE_ID}
    
    try:
        # We use verify=False because we are using a self-signed certificate.
        # The registration endpoint now acts as a seed, returning a list of peers.
        response = http_session.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        # Bootstrap our state with the list of peers from the gateway
        initial_peers = response.json().get('active_nodes', [])
        print(TC.colorize(f"Successfully contacted gateway. Seeding with peers: {initial_peers}", TC.GREEN))
        with gossip_state_lock:
            for peer_id in initial_peers:
                if peer_id not in node_states:
                    node_states[peer_id] = {"version": 0, "last_updated": time.time(), "status": "up"}
                    hash_ring.add_node(peer_id)

        return True
    except requests.exceptions.RequestException as e:
        print(TC.colorize(f"Error: Could not register with gateway at {GATEWAY_URL}. Is it running?", TC.RED))
        return False

def deregister_from_gateway():
    """Deregisters this node from the central gateway before shutdown."""
    print(TC.colorize(f"\nDeregistering node {NODE_ID} from gateway...", TC.YELLOW))
    url = f"{GATEWAY_URL}/deregister_node"
    headers = {'X-Admin-Token': ADMIN_TOKEN, 'Content-Type': 'application/json'}
    payload = {'node_id': NODE_ID}
    
    try:
        # This is now a "best effort" notification
        http_session.post(url, json=payload, headers=headers, timeout=2)
        print(TC.colorize("Node successfully deregistered.", TC.GREEN))
    except requests.exceptions.RequestException as e:
        print(TC.colorize(f"Warning: Could not deregister from gateway. It may need to be removed manually. Error: {e}", TC.YELLOW))

# --- Authentication Decorator (for Admin endpoints) ---
def require_admin_token(f):
    """A decorator to protect admin endpoints with a token."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get('X-Admin-Token') != ADMIN_TOKEN:
            return jsonify({"error": "Unauthorized: Invalid or missing admin token"}), 401
        return f(*args, **kwargs)
    return decorated_function

# --- API Endpoints for the Node ---

@app.route('/internal/kv/<key>', methods=['GET'])
def get_key(key):
    """
    Retrieves a value for a given key from the local store.
    This is called by a coordinator node during replication.
    """
    data_object = kv_store.get(key)
    if data_object: # kv_store.get returns the object or False
        # Return the entire object (value and vector clock) to the gateway
        return jsonify(data_object), 200
    else:
        return jsonify({"error": "Key not found"}), 404

@app.route('/internal/kv', methods=['POST'])
def put_key():
    """
    Stores a key-value pair and its vector clock in the local store.
    This is called by a coordinator node for replication and read-repairs.
    """
    data = request.get_json()
    # The gateway must now send a vector_clock
    if not data or 'key' not in data or 'value' not in data or 'vector_clock' not in data:
        return jsonify({"error": "Invalid request: 'key', 'value', and 'vector_clock' required"}), 400
    
    key = data['key']
    value = data['value']
    vector_clock = data['vector_clock']
    source = data.get('source', 'client') # Default to 'client' if source is not specified
    
    success = kv_store.put(key, value, vector_clock, source=source)
    
    if success:
        return jsonify({"message": f"Key '{key}' stored successfully on node {NODE_ID}"}), 201
    else:
        return jsonify({"error": f"Write rejected for key '{key}' due to stale vector clock."}), 409 # 409 Conflict

@app.route('/health', methods=['GET'])
def health_check():
    """A simple health check endpoint for the gateway to ping."""
    return jsonify({"status": "ok", "node_id": NODE_ID}), 200

@app.route('/hint', methods=['POST'])
def receive_hint():
    """Receives a hint from the gateway for a downed node."""
    data = request.get_json()
    # Vector clock is now a required part of the hint
    if not data or 'key' not in data or 'value' not in data or 'target_node' not in data or 'vector_clock' not in data:
        return jsonify({"error": "Invalid hint: 'key', 'value', 'target_node', and 'vector_clock' required"}), 400

    key = data['key']
    value = data['value']
    target_node = data['target_node']
    vector_clock = data['vector_clock']
    source = data.get('source') # Get the source field

    if hint_manager.store_hint(target_node, key, value, vector_clock, source=source):
        return jsonify({"message": f"Hint for node {target_node} stored successfully."}), 201
    else:
        return jsonify({"error": "Failed to store hint."}), 500

@app.route('/gossip', methods=['POST'])
def handle_gossip():
    """Receives and merges cluster state from another node."""
    incoming_states = request.get_json()
    if not isinstance(incoming_states, dict):
        return jsonify({"error": "Invalid gossip format"}), 400

    with gossip_state_lock:
        for node_id, state in incoming_states.items():
            current_state = node_states.get(node_id)
            
            # If we think a node is down, but we receive gossip that it is up (with a higher version),
            # we should trust it. This is a "liveness" override that helps recovery.
            is_alive_override = (
                current_state and current_state['status'] == 'down' and state['status'] == 'up'
            )

            # Update our state if we don't know about the node, their version is higher, or they were down and are now up.
            if not current_state or state['version'] > current_state['version'] or is_alive_override:
                node_states[node_id] = state
                print(TC.colorize(f"Gossip: Learned new state for {node_id} (version {state['version']}, status {state['status']}).", TC.GRAY, dim=True))

    return jsonify(node_states), 200

@app.route('/admin/cluster_state', methods=['GET'])
def get_cluster_state():
    """An admin endpoint to view this node's view of the cluster."""
    with gossip_state_lock:
        # Return a copy to avoid race conditions during serialization
        return jsonify(node_states.copy())

@app.route('/admin/list-keys', methods=['GET'])
def list_local_keys():
    """An admin endpoint to list all keys stored on this node.
    Note: This is not token-protected to allow the gateway's list-all-keys to function without a token.
    """
    local_keys = kv_store.list_keys()
    return jsonify({"keys": local_keys, "node_id": NODE_ID}), 200

@app.route('/admin/dump-data', methods=['GET'])
@require_admin_token
def dump_node_data():
    """An admin endpoint to dump all key-value data from this node."""
    # The data in kv_store.kvstore is already in memory and decrypted.
    # We return a copy to be safe against modification during serialization.
    all_data = kv_store.kvstore.copy()
    return jsonify({"node_id": NODE_ID, "data": all_data, "count": len(all_data)}), 200

@app.route('/anti-entropy/get-subtree-hashes/<parent_hash>', methods=['GET'])
def get_subtree_hashes(parent_hash):
    """Allows a peer to request the children hashes of a node in our Merkle tree."""
    # Read directly from the cached tree, which is updated by the background worker.
    merkle_tree = anti_entropy_cache.get('merkle_tree')
    if not merkle_tree:
        return jsonify({"error": "Merkle tree not yet available, please try again later."}), 503

    children_hashes = merkle_tree.get_children_hashes(parent_hash)

    if children_hashes:
        return jsonify(children_hashes), 200
    else:
        return jsonify({"error": "Parent hash not found or is a leaf"}), 404

@app.route('/anti-entropy/get-data-block/<key>', methods=['GET'])
def get_data_block_for_sync(key):
    """Allows a peer to request a specific data block for synchronization."""
    # This is identical to the normal get, but serves a different purpose.
    return get_key(key)

@app.route('/anti-entropy/get-key-for-hash/<path:leaf_hash>', methods=['GET'])
def get_key_for_hash(leaf_hash):
    """Allows a peer to ask which key corresponds to a given leaf hash."""
    merkle_tree = anti_entropy_cache.get('merkle_tree')
    if not merkle_tree:
        return jsonify({"error": "Merkle tree not yet available, please try again later."}), 503

    key = merkle_tree.leaf_hashes.get(leaf_hash)
    if key:
        return jsonify({"key": key}), 200
    else:
        return jsonify({"error": "Key for the given leaf hash not found"}), 404


# --- Client-Facing Coordinator Endpoints ---

@app.route('/client/kv/<key>', methods=['GET'])
def coordinate_get_key(key):
    """
    Acts as a coordinator for a client's GET request.
    """
    with gossip_state_lock:
        preference_list = hash_ring.get_preference_list(key, REPLICATION_FACTOR)
    if not preference_list:
        return jsonify({"error": "Could not determine preference list for key"}), 500

    print(TC.colorize(f"Coordinator: GET for '{key}'. Preference list: {preference_list}", TC.LIGHT_BLUE))

    read_results = []
    errors = []
    futures = {executor.submit(make_internal_request, node, 'GET', key): node for node in preference_list}

    for future in futures:
        node_address = futures[future]
        try:
            result, error = future.result()
            if result and 'value' in result:
                result['node_id'] = node_address
                read_results.append(result)
            else:
                errors.append(f"Node {node_address} failed or key not found: {error or 'Not found'}")
        except Exception as e:
            errors.append(f"Exception reading from {node_address}: {e}")

    if len(read_results) < READ_QUORUM:
        return jsonify({"error": f"Read quorum not met ({len(read_results)}/{READ_QUORUM})", "details": errors}), 500

    # Add a guard against the unlikely case of quorum met but no valid results
    if not read_results:
        return jsonify({"error": "Read quorum met, but no valid data was returned from replicas.", "details": errors}), 500

    # --- Conflict Resolution and Read Repair ---    
    # 1. Deduplicate identical versions that might have come from different nodes
    unique_versions = []
    seen_hashes = set()
    for v in read_results:
        # Create a stable hash of the version's content (value + sorted clock)
        version_hash = (v['value'], tuple(sorted(v['vector_clock'].items())))
        if version_hash not in seen_hashes:
            unique_versions.append(v)
            seen_hashes.add(version_hash)
    
    # 2. Filter out any versions that are ancestors of another version
    final_versions = [v1 for v1 in unique_versions if not any(compare_clocks(v1['vector_clock'], v2['vector_clock']) == 'ancestor' for v2 in unique_versions if v1 is not v2)]
    
    # 3. If more than one version remains, we have a genuine conflict
    if len(final_versions) > 1:
        return jsonify({"error": TC.colorize("Conflict detected", TC.LIGHT_RED, bold=True), "versions": final_versions}), 300

    # 4. If we are here, there is one definitive version (or the list is empty, which is handled)
    definitive_version = final_versions[0] if final_versions else read_results[0]
    
    # Perform read repair asynchronously
    nodes_that_replied = {r['node_id'] for r in read_results}
    nodes_to_repair = set(preference_list) - nodes_that_replied
    for r in read_results:
        if compare_clocks(r['vector_clock'], definitive_version['vector_clock']) == 'ancestor':
            nodes_to_repair.add(r['node_id'])

    if nodes_to_repair:
        print(TC.colorize(f"Coordinator: Read Repair for key '{key}' on nodes: {nodes_to_repair}", TC.CYAN))
        for node_id in nodes_to_repair:
            executor.submit(make_internal_request, node_id, 'POST', key, definitive_version['value'], definitive_version['vector_clock'])

    return jsonify(definitive_version), 200

@app.route('/client/kv', methods=['POST'])
def coordinate_put_key():
    """
    Acts as a coordinator for a client's POST (PUT) request.
    """
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({"error": "Invalid request: 'key' and 'value' are required"}), 400

    key, value = data['key'], data['value']
    client_clock = data.get('vector_clock', {})

    with gossip_state_lock:
        preference_list = hash_ring.get_preference_list(key, REPLICATION_FACTOR)

    if not preference_list:
        return jsonify({"error": "Could not determine preference list for key"}), 500

    print(TC.colorize(f"Coordinator: PUT for '{key}'. Preference list: {preference_list}", TC.LIGHT_GREEN))

    successful_writes = 0
    nodes_written_to = []
    failed_nodes = []
    
    # --- Read-before-write to get existing clocks ---
    # This helps create a more accurate final vector clock.
    read_results = []
    read_futures = {executor.submit(make_internal_request, node, 'GET', key): node for node in preference_list}
    for future in read_futures:
        result, _ = future.result()
        if result and 'vector_clock' in result:
            read_results.append(result)

    # Merge all clocks (client's and replicas') and increment for this new write event.
    all_clocks = [r['vector_clock'] for r in read_results] + [client_clock]
    merged_before_increment = merge_clocks(all_clocks)
    new_clock = increment_clock(merged_before_increment, NODE_ID)

    futures = {executor.submit(make_internal_request, node, 'POST', key, value, new_clock): node for node in preference_list}
    for future in futures:
        node_address = futures[future]
        try:
            result, error = future.result()
            if error: raise Exception(error)
            successful_writes += 1
            nodes_written_to.append(node_address)
        except Exception as e:
            print(TC.colorize(f"Coordinator: Failed write to primary replica {node_address}: {e}", TC.YELLOW))
            failed_nodes.append(node_address)

    # --- Sloppy Quorum and Hinted Handoff ---
    if successful_writes < WRITE_QUORUM and failed_nodes:
        with gossip_state_lock:
            all_nodes = {nid for nid, s in node_states.items() if s['status'] == 'up'}
            # The substitute pool should contain any 'up' node that is not in the original preference list.
            # A node that was successfully written to can still hold a hint for another failed node.
            substitute_pool = list(all_nodes - set(preference_list))
        
        for failed_node in list(failed_nodes):
            if successful_writes >= WRITE_QUORUM: break
            if not substitute_pool: break
            sub_node = substitute_pool.pop(0)
            print(TC.colorize(f"Coordinator: Using substitute {sub_node} for failed write to {failed_node}.", TC.LIGHT_MAGENTA))
            _, sub_error = send_hint_to_node(sub_node, key, value, new_clock, failed_node)
            if not sub_error:
                successful_writes += 1
                nodes_written_to.append(f"{sub_node} (hint for {failed_node})")
                failed_nodes.remove(failed_node)

    # Asynchronous hinted handoff for any remaining failures.
    # The coordinator stores a hint for each node that failed.
    if failed_nodes:
        print(TC.colorize(f"Coordinator: Storing hints for {len(failed_nodes)} failed node(s): {failed_nodes}", TC.YELLOW))
        for failed_node in failed_nodes:
            # The coordinator stores the hint on behalf of the failed node.
            hint_manager.store_hint(
                target_node=failed_node,
                key=key,
                value=value,
                vector_clock=new_clock,
                source='async-handoff'
            )

    if successful_writes >= WRITE_QUORUM:
        return jsonify({"message": "Write successful (quorum met)", "new_vector_clock": new_clock}), 201
    else:
        return jsonify({"error": f"Write failed: Quorum not met ({successful_writes}/{WRITE_QUORUM})"}), 500


# --- Internal Request Helpers ---

def make_internal_request(node_address, method, key, value=None, vector_clock=None):
    """Makes an HTTP request to another node's internal endpoint."""
    payload = {'key': key, 'value': value, 'vector_clock': vector_clock}
    try:
        if method == 'GET':
            url = f"https://{node_address}/internal/kv/{key}"
            response = http_session.get(url, timeout=3)
        elif method == 'POST':
            url = f"https://{node_address}/internal/kv"
            response = http_session.post(url, json=payload, timeout=5)
        else:
            return None, "Unsupported method"
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.RequestException as e:
        return None, str(e)

def send_hint_to_node(node_address, key, value, vector_clock, target_node):
    """Sends a 'hint' to a substitute node."""
    url = f"https://{node_address}/hint"
    payload = {
        'key': key, 
        'value': value, 
        'vector_clock': vector_clock, 
        'target_node': target_node,
        'source': 'hinted-handoff' # This field was missing
    }
    try:
        response = http_session.post(url, json=payload, timeout=5)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.RequestException as e:
        return None, str(e)





# --- Graceful Shutdown Handling ---

def shutdown_handler(signum, frame):
    """Handles signals like Ctrl+C for graceful shutdown."""
    if not shutdown_event.is_set():
        print(TC.colorize("\nShutdown signal received. Notifying background threads...", TC.YELLOW, bold=True))
        shutdown_event.set()
        
        # Best-effort deregistration
        deregister_from_gateway()
        
        # Give threads a moment to notice the event and shut down
        print(TC.colorize("Waiting for background threads to exit...", TC.YELLOW))
        time.sleep(2) # Wait a bit longer to ensure network calls can complete/timeout
        print("Exiting.")
        sys.exit(0) # Force exit the process.

# --- Main Execution ---

if __name__ == '__main__':
    # Suppress insecure request warnings from requests due to self-signed certs
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # The node needs the same SSL certs as the gateway to run on HTTPS
    if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
        print(f"Error: SSL certificate files '{CERT_FILE}' and '{KEY_FILE}' not found.")
        print("Please run the gateway_server.py first to generate them, or generate them manually.")
        sys.exit(1)

    ssl_context = (CERT_FILE, KEY_FILE)

    # Attempt to register with the gateway in a separate thread to not block startup.
    # This allows the server to start even if the gateway isn't ready immediately.
    def retry_registration():
        while not register_with_gateway():
            print(TC.colorize("Registration failed. Retrying in 10 seconds...", TC.YELLOW))
            time.sleep(10)

    registration_thread = threading.Thread(target=retry_registration, daemon=True)
    registration_thread.start()

    # Start the hinted handoff worker in a background thread
    handoff_thread = threading.Thread(target=hint_handoff_worker, args=(hint_manager, node_states, gossip_state_lock, shutdown_event, http_session), daemon=True)
    handoff_thread.start()
    print(TC.colorize("Started background hinted handoff worker.", TC.MAGENTA))
    
    # Start the gossip worker
    gossip_thread = threading.Thread(target=gossip_worker, args=(NODE_ID, node_states, gossip_state_lock, hash_ring, GOSSIP_INTERVAL, shutdown_event, http_session, 3), daemon=True)
    gossip_thread.start()
    print(TC.colorize("Started background gossip worker.", TC.GRAY, dim=True))

    # Start the anti-entropy worker
    anti_entropy_thread = threading.Thread(target=anti_entropy_worker, args=(kv_store, node_states, gossip_state_lock, shutdown_event, http_session, anti_entropy_cache), daemon=True)
    anti_entropy_thread.start()
    print(TC.colorize("Started background anti-entropy worker.", TC.CYAN))

    print(TC.colorize("--- Starting KVStore Node ---", TC.GREEN, bold=True))
    print(TC.colorize(f"Node ID: {NODE_ID}", TC.GREEN))
    print(TC.colorize(f"Data Directory: {DATA_DIR}", TC.GREEN))
    print(TC.colorize(f"Gateway: {GATEWAY_URL}", TC.GREEN))
    print(TC.colorize(f"Serving on https://{NODE_HOST}:{NODE_PORT}", TC.GREEN))
    print(TC.colorize("Press Ctrl+C to shut down gracefully.", TC.YELLOW))
    
    # Run the Flask app with SSL and multithreading enabled
    app.run(host='0.0.0.0', port=NODE_PORT, ssl_context=ssl_context, threaded=True)
