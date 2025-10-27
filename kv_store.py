import json
from vector_clock_utils import compare_clocks
from terminal_colors import TC

class KVStore:
    def __init__(self, data_dir="data", node_id="0", cipher=None):
        self.kvstore = {} # Maps key -> {'value': value, 'vector_clock': clock}
        self.wal_path = f"{data_dir}/{node_id.replace(':', '_')}_log.log"
        self.node_id = node_id # Store the node ID for reference
        self.cipher = cipher

        from os import makedirs
        makedirs(data_dir, exist_ok=True)
        
        self.recover_from_wal()
    
    def recover_from_wal(self):
        try:
            with open(self.wal_path, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    
                    decrypted_line = line.strip()
                    if self.cipher:
                        decrypted_line = self.cipher.decrypt(decrypted_line.encode('utf-8')).decode('utf-8')

                    try:
                        data = json.loads(decrypted_line)
                        key = data['key']
                        self.kvstore[key] = {
                            'value': data['value'],
                            'vector_clock': data['vector_clock']
                        }
                    except (json.JSONDecodeError, KeyError):
                        print(TC.colorize(f"Skipping malformed log entry: {line.strip()}", TC.YELLOW))
            print(TC.colorize(f"Recovery complete. Loaded {len(self.kvstore)} keys from WAL.", TC.GREEN))

        except FileNotFoundError:
            print(TC.colorize("Log not found. Starting with an empty state.", TC.YELLOW))


    def put(self, key, value, vector_clock, source='client'):
        # Prevent stale writes by checking vector clocks.
        if key in self.kvstore:
            stored_clock = self.kvstore[key]['vector_clock']
            comparison = compare_clocks(vector_clock, stored_clock)
            
            # Only write if the new clock is a descendant or equal.
            # A write should only be rejected if it is a strict ancestor of what's already stored.
            if comparison == 'ancestor':
                print(TC.colorize(f"Rejected stale write for key='{key}'. Incoming clock is not a descendant.", TC.YELLOW))
                return False # Indicate that the write was rejected

        log_entry = {
            'key': key,
            'value': value,
            'vector_clock': vector_clock
        }
        
        log_entry_str = json.dumps(log_entry)
        
        if self.cipher:
            log_entry_str = self.cipher.encrypt(log_entry_str.encode('utf-8')).decode('utf-8')

        with open(self.wal_path, "a") as f:
            f.write(log_entry_str + "\n")
            
        self.kvstore[key] = {'value': value, 'vector_clock': vector_clock}
        
        if source == 'hinted-handoff':
            print(TC.colorize(f"Successfully put key='{key}' (from hinted handoff)", TC.MAGENTA))
        else:
            print(f"Successfully put key='{key}'") # Standard client writes can be default color
        return True


    def get(self, key):
        if key in self.kvstore:
            # Return the whole object including value and clock
            return self.kvstore[key]
        else:
            return False

    def list_keys(self):
        """Returns a list of all keys stored locally."""
        return list(self.kvstore.keys())
