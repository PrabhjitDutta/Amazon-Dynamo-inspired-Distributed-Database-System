import hashlib
import json

class MerkleNode:
    """Represents a single node in the Merkle Tree."""
    def __init__(self, hash_val):
        self.hash = hash_val
        self.left = None
        self.right = None

def _hash_func(data):
    """
    A consistent hashing function for our tree.
    Handles bytes, strings, and dictionaries.
    """
    if isinstance(data, bytes):
        return hashlib.sha256(data).hexdigest()
    
    # For our KV store, data will be like {'key': k, 'value': v, 'vector_clock': vc}
    # We must serialize it consistently to get a stable hash.
    if isinstance(data, dict):
        # Sort keys to ensure consistent serialization
        serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
        return hashlib.sha256(serialized_data).hexdigest()
    
    return hashlib.sha256(str(data).encode('utf-8')).hexdigest()

class MerkleTree:
    """
    Implements a Merkle Tree for verifying data integrity.
    """
    def __init__(self, data_blocks=None):
        """
        Initializes and builds the Merkle Tree.

        Args:
            data_blocks (list, optional): A list of data blocks to build the tree from.
                                          For our KV store, this would be a list of
                                          {'key': k, 'value': v, 'vector_clock': vc} dicts.
        """
        self.leaves = []
        self.root = None
        self.nodes = {} # Maps hash -> MerkleNode object for easy lookup
        self.leaf_hashes = {} # Maps leaf hash -> key for efficient sync
        if data_blocks:
            self._build_tree(data_blocks)

    def get_root_hash(self):
        """Returns the hash of the root node, or None if the tree is empty."""
        return self.root.hash if self.root else None

    def get_children_hashes(self, parent_hash):
        """Returns the hashes of the children of a given parent hash."""
        parent_node = self.nodes.get(parent_hash)
        if parent_node and parent_node.left and parent_node.right:
            return {
                "left": parent_node.left.hash,
                "right": parent_node.right.hash
            }
        return None

    def _build_tree(self, data_blocks):
        """Constructs the Merkle Tree from a list of data blocks."""
        if not data_blocks:
            return

        # Create leaf nodes from the initial data blocks.
        # We sort the data blocks by key to ensure the tree is built consistently
        # across different nodes that hold the same set of keys.
        sorted_blocks = sorted(data_blocks, key=lambda x: x['key'])
        
        current_level = []
        for block in sorted_blocks:
            node = MerkleNode(_hash_func(block))
            current_level.append(node)
            self.leaf_hashes[node.hash] = block['key']
        self.leaves = current_level

        # Iteratively build the tree level by level until only the root remains.
        while len(current_level) > 1:
            # If the current level has an odd number of nodes, duplicate the last one.
            if len(current_level) % 2 != 0:
                current_level.append(current_level[-1])

            next_level = []
            # Process nodes in pairs to create the parents for the next level up.
            for i in range(0, len(current_level), 2):
                left_child = current_level[i]
                right_child = current_level[i+1]

                # The parent's hash is the hash of its children's concatenated hashes.
                parent_hash_data = (left_child.hash + right_child.hash).encode('utf-8')
                parent_node = MerkleNode(_hash_func(parent_hash_data))
                parent_node.left = left_child
                parent_node.right = right_child
                self.nodes[parent_node.hash] = parent_node
                
                next_level.append(parent_node)
            
            current_level = next_level

        # The last remaining node is the root of the tree.
        self.root = current_level[0] if current_level else None
        if self.root:
            self.nodes[self.root.hash] = self.root


# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    # Simulate some data from two different nodes for the same key range.
    # Node 1 has the correct data.
    node1_data = [
        {'key': 'key1', 'value': 'value1', 'vector_clock': {'n1': 1}},
        {'key': 'key2', 'value': 'value2', 'vector_clock': {'n1': 1}},
        {'key': 'key3', 'value': 'value3', 'vector_clock': {'n1': 1}},
    ]

    # Node 2 has one inconsistent key ('key2').
    node2_data = [
        {'key': 'key1', 'value': 'value1', 'vector_clock': {'n1': 1}},
        {'key': 'key2', 'value': 'stale_value', 'vector_clock': {'n1': 0}}, # Inconsistent
        {'key': 'key3', 'value': 'value3', 'vector_clock': {'n1': 1}},
    ]

    # Build Merkle trees for both nodes.
    tree1 = MerkleTree(node1_data)
    tree2 = MerkleTree(node2_data)

    print(f"Node 1 Root Hash: {tree1.get_root_hash()}")
    print(f"Node 2 Root Hash: {tree2.get_root_hash()}")
    print("-" * 30)
    print(f"Hashes are different: {tree1.get_root_hash() != tree2.get_root_hash()}")
    print("This difference indicates the nodes are out of sync and need to compare sub-trees.")