import os
import sys
import json
import requests
import threading
from flask import Flask, request, jsonify
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
import subprocess

# Ensure Hash_Ring is importable. Assumes Hash_Ring.py is in the same directory.
from Hash_Ring import HashRing

# --- Configuration ---
GATEWAY_PORT = 8000
REPLICATION_FACTOR = 3 # Needed for the /admin/locate-key endpoint
VIRTUAL_NODES_PER_SERVER = 256

# --- Security Configuration ---
ADMIN_TOKEN = "password" # CHANGE THIS IN PRODUCTION

# --- Flask App Initialization ---
app = Flask(__name__)
# Initialize HashRing with no nodes initially; they will register themselves
hash_ring = HashRing(nodes=[], vnodes_per_server=VIRTUAL_NODES_PER_SERVER)

# --- Thread-Safe State Management ---
# A lock to protect access to the hash_ring and node_health_status
state_lock = threading.Lock() # Lock still needed for ring modifications

# --- Authentication Decorator ---
def require_admin_token(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get('X-Admin-Token') != ADMIN_TOKEN:
            return jsonify({"error": "Unauthorized: Invalid or missing admin token"}), 401
        return f(*args, **kwargs)
    return decorated_function

# --- New Coordinator Locator Endpoint ---
@app.route('/locate-coordinator/<key>', methods=['GET'])
def locate_coordinator(key):
    """
    Returns the address of the primary node (coordinator) for a given key.
    This allows the client to talk directly to the responsible node.
    """
    # REPLICATION_FACTOR is needed here to get the full preference list
    global REPLICATION_FACTOR 

    with state_lock:
        if not hash_ring._ring:
            return jsonify({"error": "No active nodes in the cluster."}), 503
        
        # Return the entire preference list, so the client can try multiple nodes
        preference_list = hash_ring.get_preference_list(key, REPLICATION_FACTOR)

    if not preference_list:
        return jsonify({"error": f"Could not determine preference list for key '{key}'"}), 500

    # The first node in the preference list is the primary coordinator
    return jsonify({"preference_list": preference_list, "primary_coordinator": preference_list[0]}), 200
# --- Gateway Endpoints ---

@app.route('/register_node', methods=['POST'])
@require_admin_token
def register_node():
    """
    Endpoint for new nodes to get a list of peers to start gossiping with.
    Expected JSON: {"node_id": "ip:port"}
    """
    data = request.get_json()
    if not data or 'node_id' not in data:
        return jsonify({"error": "Invalid request: 'node_id' missing"}), 400
    
    node_id = data['node_id']
    
    with state_lock:
        current_nodes = list(hash_ring.get_all_physical_nodes())
        # Add the new node to the ring immediately
        if node_id not in current_nodes:
            hash_ring.add_node(node_id)
            print(f"Registered new KVStore node: {node_id}. Current nodes: {list(hash_ring.get_all_physical_nodes())}")
        else:
            print(f"KVStore node {node_id} already registered.")

    return jsonify({"message": f"Node {node_id} registered successfully", "active_nodes": list(hash_ring.get_all_physical_nodes())}), 200

@app.route('/deregister_node', methods=['POST'])
@require_admin_token
def deregister_node():
    """Endpoint for a node to gracefully deregister itself before shutting down."""
    data = request.get_json()
    if not data or 'node_id' not in data:
        return jsonify({"error": "Invalid request: 'node_id' missing"}), 400
    
    node_id = data['node_id']
    with state_lock:
        # The gateway learns about node removal from the node itself or via gossip updates.
        # This endpoint is now just a notification.
        if node_id in hash_ring.get_all_physical_nodes():
            hash_ring.remove_node(node_id)
            print(f"Node {node_id} deregistered. Current nodes: {list(hash_ring.get_all_physical_nodes())}")
            return jsonify({"message": f"Node {node_id} deregistered successfully."}), 200
    return jsonify({"error": f"Node {node_id} not found."}), 404

@app.route('/admin/list-all-keys', methods=['GET'])
@require_admin_token
def list_all_keys():
    """
    Gathers and returns a list of all unique keys from all active nodes.
    This is an expensive operation and should be used for administrative purposes only.
    """
    with state_lock:
        all_nodes = list(hash_ring.get_all_physical_nodes())

    if not all_nodes:
        return jsonify({"keys": [], "message": "No active nodes in the cluster."}), 200

    all_keys = set()
    futures = []
    with ThreadPoolExecutor(max_workers=len(all_nodes) or 1) as key_executor:
        for node_id in all_nodes:
            url = f"https://{node_id}/admin/list-keys"
            futures.append(key_executor.submit(requests.get, url, timeout=5, verify=False))

        for future in futures:
            try:
                response = future.result()
                if response.status_code == 200:
                    all_keys.update(response.json().get('keys', []))
            except requests.exceptions.RequestException as e:
                print(f"Admin: Failed to get keys from a node: {e}")

    return jsonify({"keys": sorted(list(all_keys)), "key_count": len(all_keys)}), 200

@app.route('/admin/locate-key/<key>', methods=['GET'])
@require_admin_token
def locate_key(key):
    """
    Returns the preference list for a given key, showing which nodes are responsible for it.
    """
    with state_lock:
        if not hash_ring._ring:
            return jsonify({"error": "No active nodes in the cluster."}), 503
        
        # Get the ideal preference list
        preference_list = hash_ring.get_preference_list(key, REPLICATION_FACTOR)
        primary_node = hash_ring.get_node(key)
        # Get the current set of nodes the gateway believes are online
        active_nodes = set(hash_ring.get_all_physical_nodes())

    if not preference_list:
        return jsonify({"error": f"Could not determine preference list for key '{key}'"}), 500

    # Enrich the preference list with the current status of each node
    enriched_preference_list = [
        {"node_id": node, "status": "up" if node in active_nodes else "down"}
        for node in preference_list
    ]

    return jsonify({
        "key": key,
        "primary_node": primary_node,
        "preference_list": enriched_preference_list
    }), 200

# --- SSL Context for HTTPS ---
# For a real production environment, you would use proper certificates.
# For a local test, we can generate a self-signed cert.
# You can generate these with:
# openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365 -subj "/CN=localhost"

# Define paths relative to the script's directory for robustness
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CERT_FILE = os.path.join(SCRIPT_DIR, 'cert.pem')
KEY_FILE = os.path.join(SCRIPT_DIR, 'key.pem')

def generate_self_signed_cert():
    """Generates a self-signed certificate for local testing if not present."""
    if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
        print(f"Generating self-signed SSL certificate ({CERT_FILE}, {KEY_FILE})...")
        try:
            # Use subprocess for better error handling. Requires openssl to be in the system's PATH.
            command = [
                'openssl', 'req', '-x509', '-newkey', 'rsa:4096', '-nodes',
                '-out', CERT_FILE, '-keyout', KEY_FILE,
                '-days', '365', '-subj', '/CN=localhost'
            ]
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            
            if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
                raise RuntimeError("openssl ran but failed to create certificate files.")
            
            print("Certificate generated successfully.")
        except Exception as e:
            print(f"\n--- SSL Certificate Generation Failed ---")
            print(f"Error: {e}")
            print("Please ensure 'openssl' is installed and accessible in your system's PATH.")
            print("You might need to generate them manually or run without SSL for testing.")
            sys.exit(1) # Exit immediately as HTTPS is required for nodes
    return True

# --- Main Execution ---
if __name__ == '__main__':
    # Suppress insecure request warnings from requests due to self-signed certs
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Generate certs if needed
    if not generate_self_signed_cert():
        ssl_context = None
    else:
        ssl_context = (CERT_FILE, KEY_FILE)

    print(f"Starting Gateway Admin & Bootstrap Server on HTTPS port {GATEWAY_PORT}...")
    print(f"Admin Token: {ADMIN_TOKEN[:4]}... (use this in X-Admin-Token header for node management)")
    
    # Flask's development server can run with SSL and in threaded mode
    # For production, use a WSGI server like Gunicorn with appropriate workers.
    app.run(host='0.0.0.0', port=GATEWAY_PORT, ssl_context=ssl_context, threaded=True)
