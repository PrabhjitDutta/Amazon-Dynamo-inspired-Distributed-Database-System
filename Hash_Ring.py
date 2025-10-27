import hashlib
import bisect

class HashRing:
    """
    Implements a consistent hash ring with virtual nodes.
    
    This class allows for the distribution of keys among a set of servers (nodes).
    It uses virtual nodes to ensure a more uniform distribution of data.
    """
    
    def __init__(self, nodes=None, vnodes_per_server=256):
        """
        Initializes the hash ring.
        
        Args:
            nodes (list, optional): A list of initial node IDs. Defaults to None.
            vnodes_per_server (int, optional): The number of virtual nodes for each 
                                             physical server. Defaults to 128.
        """
        self.vnodes_per_server = vnodes_per_server
        self._ring = {}  # A dictionary mapping hash -> physical_node_id
        self._sorted_keys = []  # A sorted list of hashes for efficient lookup
        
        if nodes:
            for node_id in nodes:
                self.add_node(node_id)


    def _hash(self, key_str):
        """Hashes a string key to an integer."""
        # Use MD5 as requested and take the first 8 bytes for a 32-bit integer space
        return int(hashlib.md5(key_str.encode('utf-8')).hexdigest(), 16)
    

    def add_node(self, node_id):
        """
        Adds a physical node to the hash ring.
        
        Args:
            node_id (str): The unique identifier for the server (e.g., '192.168.1.101').
        """
        for i in range(self.vnodes_per_server):
            # Create a unique identifier for each virtual node
            vnode_key = f"{node_id}:{i}"
            key_hash = self._hash(vnode_key)
            
            # Map the hash to the physical node ID
            self._ring[key_hash] = node_id
            # Add the hash to our sorted list
            bisect.insort(self._sorted_keys, key_hash)


    def remove_node(self, node_id):
        """
        Removes a physical node and all its virtual nodes from the ring.
        
        Args:
            node_id (str): The unique identifier for the server to be removed.
        """
        for i in range(self.vnodes_per_server):
            vnode_key = f"{node_id}:{i}"
            key_hash = self._hash(vnode_key)
            
            # Remove from both the mapping and the sorted list
            if key_hash in self._ring:
                del self._ring[key_hash]
                # This is an O(n) operation, but node removal is infrequent
                self._sorted_keys.remove(key_hash)


    def get_node(self, key):
        """
        Finds the primary physical node responsible for a given key.
        
        Args:
            key (str): The key to be looked up (e.g., a user ID or session ID).
            
        Returns:
            str: The ID of the responsible physical node.
        """
        if not self._ring:
            return None
            
        key_hash = self._hash(key)
        
        # Use binary search to find the position of the first vnode >= the key's hash
        position = bisect.bisect_left(self._sorted_keys, key_hash)
        
        # Handle the wrap-around case where the key's hash is larger than any vnode hash
        if position == len(self._sorted_keys):
            position = 0
            
        target_hash = self._sorted_keys[position]
        return self._ring[target_hash]
    

    def get_preference_list(self, key, N):
        """
        Finds the N unique physical nodes responsible for a given key.
        
        Args:
            key (str): The key to be looked up.
            N (int): The number of unique nodes to find (the replication factor).
            
        Returns:
            list: A list of N unique physical node IDs.
        """
        if not self._ring:
            return []

        # If N is greater than the number of unique nodes, adjust N to return all nodes.
        N = min(N, len(set(self._ring.values())))

        preference_list = []
        key_hash = self._hash(key)
        position = bisect.bisect_left(self._sorted_keys, key_hash)
        
        # Walk the ring clockwise
        for i in range(len(self._sorted_keys)):
            # Start from the found position and wrap around if necessary
            current_pos = (position + i) % len(self._sorted_keys)
            target_hash = self._sorted_keys[current_pos]
            node_id = self._ring[target_hash]
            
            # Add the node to the list only if it's not already there
            if node_id not in preference_list:
                preference_list.append(node_id)
            
            # Stop once we have found N unique nodes
            if len(preference_list) == N:
                break
                
        return preference_list
    

    def get_all_physical_nodes(self):
        """
        Returns a list of all unique physical nodes in the ring.
        
        Returns:
            list: A list of unique physical node IDs.
        """
        return list(set(self._ring.values()))
