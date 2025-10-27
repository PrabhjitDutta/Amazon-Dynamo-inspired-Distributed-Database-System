def compare_clocks(vc1, vc2):
    """Compares two vector clocks. Returns 'descendant', 'ancestor', 'concurrent', or 'equal'."""
    vc1_greater = False
    vc2_greater = False
    all_keys = set(vc1.keys()) | set(vc2.keys())

    for key in all_keys:
        v1 = vc1.get(key, 0)
        v2 = vc2.get(key, 0)
        if v1 > v2:
            vc1_greater = True
        elif v2 > v1:
            vc2_greater = True
    
    if vc1_greater and not vc2_greater:
        return 'descendant' # vc1 is a descendant of vc2
    elif vc2_greater and not vc1_greater:
        return 'ancestor' # vc1 is an ancestor of vc2
    elif vc1_greater and vc2_greater:
        return 'concurrent' # Conflict
    else:
        return 'equal'

def merge_clocks(clocks):
    """Merges a list of vector clocks into a single clock with the max values."""
    merged = {}
    for clock in clocks:
        for node, time in clock.items():
            merged[node] = max(merged.get(node, 0), time)
    return merged

def increment_clock(clock, node_id):
    """Increments the clock for a given node_id."""
    new_clock = clock.copy()
    new_clock[node_id] = new_clock.get(node_id, 0) + 1
    return new_clock