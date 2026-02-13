import random
import statistics
import collections
from simple_hashing import ConsistentHashRing  # Assuming your class is in consistent_hash.py

def measure_distribution(ring, num_keys=10000):
    """
    Generates 'num_keys' random strings and assigns them to nodes.
    Returns: A dictionary of {NodeName: KeyCount}
    """
    node_counts = collections.defaultdict(int)
    
    # 1. Generate random keys and assign them
    for i in range(num_keys):
        # Create a random key (e.g., "user_84920")
        key = f"user_{random.randint(0, 1_000_000)}"
        
        # 2. Ask the ring: "Who owns this?"
        assigned_node = ring.get_node(key)
        
        # 3. Count it
        node_counts[assigned_node] += 1
    
    return node_counts

def print_stats(counts):
    """
    Calculates Mean and Standard Deviation to grade the balance.
    """
    values = list(counts.values())
    mean = statistics.mean(values)
    stdev = statistics.stdev(values)
    pct_stdev = (stdev / mean) * 100
    
    print(f"  - Nodes: {len(counts)}")
    print(f"  - Mean Keys per Node: {mean:.2f}")
    print(f"  - Std Dev: {stdev:.2f} ({pct_stdev:.2f}%)")
    print(f"  - Max Node: {max(values)}")
    print(f"  - Min Node: {min(values)}")
    return pct_stdev

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    KEYS_TO_TEST = 10000
    V_NODES = 100  # Try changing this to 1 or 10 later to see the difference!

    print(f"=== TEST 1: Initial Distribution (vNodes={V_NODES}) ===")
    ring = ConsistentHashRing(num_virtual_nodes=V_NODES)
    
    # Add 5 Nodes
    nodes = ["Node_A", "Node_B", "Node_C", "Node_D", "Node_E"]
    for n in nodes:
        ring.add_node(n)
        
    counts = measure_distribution(ring, KEYS_TO_TEST)
    print_stats(counts)

    print("\n=== TEST 2: Node Addition (Rebalancing) ===")
    # 1. Track where a specific set of keys is right now
    # We use a fixed set of keys to track movement precisely
    test_keys = [f"key_{i}" for i in range(KEYS_TO_TEST)]
    initial_assignment = {}
    for k in test_keys:
        initial_assignment[k] = ring.get_node(k)
        
    # 2. Add a NEW Node
    print(">>> Adding 'Node_F'...")
    ring.add_node("Node_F")
    
    # 3. Check where the keys are now
    moved_keys = 0
    new_counts = collections.defaultdict(int)
    
    for k in test_keys:
        new_node = ring.get_node(k)
        new_counts[new_node] += 1
        if new_node != initial_assignment[k]:
            moved_keys += 1
            
    print(f"  - Total Keys: {KEYS_TO_TEST}")
    print(f"  - Keys Moved: {moved_keys}")
    print(f"  - Percent Moved: {(moved_keys/KEYS_TO_TEST)*100:.2f}%")
    print(f"  - Ideal Movement (1/N): {(1/6)*100:.2f}%") # 1/6 because we now have 6 nodes
    
    print("\n=== TEST 3: New Distribution After Add ===")
    print_stats(new_counts)