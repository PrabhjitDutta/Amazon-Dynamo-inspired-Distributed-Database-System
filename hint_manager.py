import os
import json

class HintManager:
    def __init__(self, data_dir, cipher=None):
        self.hint_dir = os.path.join(data_dir, 'hints')
        os.makedirs(self.hint_dir, exist_ok=True)
        self.cipher = cipher

    def _get_hint_path(self, target_node):
        # Sanitize node ID to be a valid filename
        filename = target_node.replace(':', '_') + '.json'
        return os.path.join(self.hint_dir, filename)

    def store_hint(self, target_node, key, value, vector_clock, source=None):
        """Stores a hint for a downed node."""
        hint_path = self._get_hint_path(target_node)
        try:
            with open(hint_path, 'a') as f:
                hint_entry_str = json.dumps({'key': key, 'value': value, 'vector_clock': vector_clock, 'source': source})
                
                if self.cipher:
                    hint_entry_str = self.cipher.encrypt(hint_entry_str.encode('utf-8')).decode('utf-8')

                # Store each hint as a JSON line for easy reading
                f.write(hint_entry_str + '\n')

            print(f"Stored hint for {target_node}: key='{key}'")
            return True
        except IOError as e:
            print(f"Error storing hint for {target_node}: {e}")
            return False

    def get_and_clear_hints(self, target_node):
        """Atomically reads and deletes hints for a given node."""
        hint_path = self._get_hint_path(target_node)
        if not os.path.exists(hint_path):
            return []
        
        hints = []
        temp_path = hint_path + '.tmp'
        try:
            # Atomic operation: rename file, read from it, then delete
            os.rename(hint_path, temp_path)
            with open(temp_path, 'r') as f:
                for line in f:
                    if line.strip():
                        decrypted_line = line.strip()
                        if self.cipher:
                            decrypted_line = self.cipher.decrypt(decrypted_line.encode('utf-8')).decode('utf-8')
                        
                        hints.append(json.loads(decrypted_line))

            os.remove(temp_path)
            print(f"Retrieved and cleared {len(hints)} hints for {target_node}.")
            return hints
        except (IOError, json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error processing hints for {target_node}: {e}")
            # If something went wrong, try to restore the original file
            if os.path.exists(temp_path):
                os.rename(temp_path, hint_path)
            return []

    def get_all_hinted_nodes(self):
        """Returns a list of all nodes for which we have hints."""
        files = os.listdir(self.hint_dir)
        # Convert filenames back to 'ip:port' format
        nodes = [f.replace('_', ':').replace('.json', '') for f in files if f.endswith('.json')]
        return nodes