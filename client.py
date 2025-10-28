import os
import sys
import requests
import random
import json
from vector_clock_utils import merge_clocks

# --- Configuration ---
GATEWAY_URL = "https://localhost:8000"  # The client only needs to know the gateway.
CONTEXT_DIR = ".client_context" # Directory to store vector clocks locally

def get_context_path(key):
    sanitized_key = "".join(c for c in key if c.isalnum() or c in ('-', '_')).rstrip()
    return os.path.join(CONTEXT_DIR, f"{sanitized_key}.json")

def save_context(key, data):
    """Saves the response data (including vector clock) for a key."""
    os.makedirs(CONTEXT_DIR, exist_ok=True)
    with open(get_context_path(key), 'w') as f:
        json.dump(data, f, indent=2)

def get_coordinator_for_key(key):
    """Asks the gateway for the preference list for a specific key."""
    url = f"{GATEWAY_URL}/locate-coordinator/{key}"
    try:
        response = requests.get(url, verify=False, timeout=3)
        response.raise_for_status()
        return response.json().get('preference_list'), None
    except requests.exceptions.RequestException as e:
        return None, str(e)

def get_key(key, silent=False):
    """Sends a GET request to the correct coordinator node to retrieve a key."""
    preference_list, error = get_coordinator_for_key(key)
    if error:
        print(f"Error: Could not get coordinator from gateway: {error}")
        return
    if not preference_list:
        print(f"Error: Gateway returned an empty preference list for key '{key}'.")
        return

    # Try each node in the preference list as a coordinator
    for coordinator_node in preference_list:
        url = f"https://{coordinator_node}/client/kv/{key}"
        if not silent:
            print(f"Attempting GET request to coordinator: {url}")
        try:
            response = requests.get(url, verify=False, timeout=5) # Add a timeout
            
            if not silent:
                print(f"\n--- Response from {coordinator_node} ---")
                print(f"Status Code: {response.status_code}")
            response_data = response.json()

            if response.status_code == 200:
                if not silent:
                    print("Successfully retrieved key.")
                    print(f"Value: {response_data.get('value')}")
                save_context(key, response_data)
                print(f"Context (vector clock) for '{key}' has been saved/updated.")
                return # Success, exit function
            elif response.status_code == 300: # Conflict
                print("\nDATA CONFLICT DETECTED!")
                print("Multiple versions of the key exist. Please resolve the conflict.")
                print(json.dumps(response_data, indent=2))
                save_context(key, response_data)
                print(f"\nConflicting versions saved. Use the 'resolve' command to fix.")
                print(f"Example: python client.py resolve {key} <merged_value>")
                return # Conflict detected, exit function
            elif response.status_code == 404:
                print("Key not found.")
                return # Key not found, exit function
            else:
                print("An error occurred:")
                print(json.dumps(response_data, indent=2))
                # Continue to next coordinator if this one returned an error other than 404/300

        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not connect to coordinator {coordinator_node} at {url}. Trying next in preference list. Details: {e}")
            # Continue to the next coordinator in the list
    
    print(f"\nError: Failed to complete GET request for key '{key}'. All potential coordinators in the preference list were unresponsive or returned an error.")

def put_key(key, value, vector_clock_override=None):
    """Sends a POST request to the correct coordinator node to store a key-value pair."""
    preference_list, error = get_coordinator_for_key(key)
    if error:
        print(f"Error: Could not get coordinator from gateway: {error}")
        return
    if not preference_list:
        print(f"Error: Gateway returned an empty preference list for key '{key}'.")
        return
    
    if vector_clock_override is not None:
        payload = {'key': key, 'value': value, 'vector_clock': vector_clock_override}
    else:
        payload = {'key': key, 'value': value, 'vector_clock': {}}
        # Try to load context from a previous GET
        context_path = get_context_path(key)
        if os.path.exists(context_path):
            with open(context_path, 'r') as f:
                context_data = json.load(f)
            if 'versions' in context_data:
                print(f"Error: Key '{key}' is in a conflict state. Please use 'resolve' command first.")
                return
            payload['vector_clock'] = context_data.get('vector_clock', {})

    headers = {'Content-Type': 'application/json'}
    
    # Try each node in the preference list as a coordinator
    for coordinator_node in preference_list:
        url = f"https://{coordinator_node}/client/kv"
        print(f"Attempting POST request to coordinator: {url} with payload: {payload}")
        try:
            response = requests.post(url, json=payload, headers=headers, verify=False, timeout=5) # Add a timeout

            print(f"\n--- Response from {coordinator_node} ---")
            print(f"Status Code: {response.status_code}")
            response_data = response.json()
            print(json.dumps(response_data, indent=2))

            # If write was successful, save the new context returned by the coordinator.
            if response.status_code == 201:
                new_context = {
                    'value': value,
                    'vector_clock': response_data.get('new_vector_clock', {})
                }
                save_context(key, new_context)
                print(f"\nWrite successful. Context for '{key}' has been updated.")
                return # Success, exit function

        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not connect to coordinator {coordinator_node} at {url}. Trying next in preference list. Details: {e}")
            # Continue to the next coordinator in the list
    
    print(f"\nError: Failed to complete PUT request for key '{key}'. All potential coordinators in the preference list were unresponsive or returned an error.")

def resolve_key(key, merged_value):
    """Sends a PUT request with a merged vector clock to resolve a conflict."""
    context_path = get_context_path(key)
    if not os.path.exists(context_path):
        print(f"Error: No context found for key '{key}'. Please 'get' the key first to identify the conflict.")
        return

    with open(context_path, 'r') as f:
        context_data = json.load(f)
    
    if 'versions' not in context_data:
        print(f"Error: Key '{key}' is not in a conflict state according to local context.")
        return

    # Merge the vector clocks from all conflicting versions
    clocks_to_merge = [v['vector_clock'] for v in context_data['versions']]
    # Use the imported utility function
    merged_clock = merge_clocks(clocks_to_merge)
    
    print(f"Resolving conflict for key '{key}' with merged clock: {merged_clock}")
    
    # Now, perform a standard PUT with the user's merged value and the new clock
    # We can reuse the put_key function by passing the merged clock as an override
    put_key(key, merged_value, vector_clock_override=merged_clock)

def list_all_keys(admin_token):
    """Sends a request to the gateway to list all keys in the cluster."""
    url = f"{GATEWAY_URL}/admin/list-all-keys"
    headers = {'X-Admin-Token': admin_token}
    print(f"Sending GET request to: {url}")
    try:
        response = requests.get(url, headers=headers, verify=False)
        print("\n--- Gateway Response ---")
        print(f"Status Code: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except requests.exceptions.RequestException as e:
        print(f"\nError: Could not connect to the gateway at {url}.")
        print(f"Please ensure the gateway_server.py is running and accessible.")
        print(f"Details: {e}")

def locate_key(key, admin_token):
    """Sends a request to the gateway to find the preference list for a key."""
    url = f"{GATEWAY_URL}/admin/locate-key/{key}"
    headers = {'X-Admin-Token': admin_token}
    print(f"Sending GET request to: {url}")
    try:
        response = requests.get(url, headers=headers, verify=False)
        print("\n--- Gateway Response ---")
        print(f"Status Code: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except requests.exceptions.RequestException as e:
        print(f"\nError: Could not connect to the gateway at {url}.")
        print(f"Please ensure the gateway_server.py is running and accessible.")
        print(f"Details: {e}")

def dump_node_data(node_id, admin_token):
    """Sends a request to a specific node to dump all of its data."""
    url = f"https://{node_id}/admin/dump-data"
    headers = {'X-Admin-Token': admin_token}
    print(f"Sending GET request to: {url}")
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        print("\n--- Node Response ---")
        print(f"Status Code: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
    except requests.exceptions.RequestException as e:
        print(f"\nError: Could not connect to the node at {url}.")
        print(f"Please ensure the node is running and accessible.")
        print(f"Details: {e}")


def print_usage():
    """Prints the command-line usage instructions."""
    print("--- Distributed KV Store Client ---")
    print("Usage:")
    print("  python client.py get <key>")
    print("  python client.py put <key> <value>      (Updates a key using saved context)")
    print("  python client.py resolve <key> <value>  (Resolves a conflict with a new value)")
    print("\nAdmin Commands:")
    print("  python client.py list-keys <admin_token> (Lists all keys in the cluster)")
    print("  python client.py locate-key <key> <admin_token> (Finds which nodes are responsible for a key)")
    print("  python client.py dump-node <node_id> <admin_token> (Dumps all data from a specific node)")
    print("\nExamples:")
    print("  python client.py get my_favorite_key")
    print("  python client.py put user:123 '{\"name\": \"John Doe\", \"email\": \"john@example.com\"}'")

if __name__ == '__main__':
    # Suppress the warning about insecure requests due to verify=False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    args = sys.argv[1:]
    if not args:
        print_usage()
        sys.exit(1)

    command = args[0].lower()

    if command == 'get' and len(args) == 2:
        get_key(args[1])
    elif command == 'put' and len(args) == 3:
        put_key(args[1], args[2])
    elif command == 'resolve' and len(args) == 3:
        resolve_key(args[1], args[2])
    elif command == 'list-keys' and len(args) == 2:
        list_all_keys(args[1])
    elif command == 'locate-key' and len(args) == 3:
        locate_key(args[1], args[2])
    elif command == 'dump-node' and len(args) == 3:
        dump_node_data(args[1], args[2])
    else:
        print(f"Error: Invalid command or number of arguments for '{command}'.\n")
        print_usage()
        sys.exit(1)