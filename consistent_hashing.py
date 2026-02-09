import hashlib
import bisect
import struct
from collections import defaultdict

# --- 1. HashFunction Module ---
class HashFunction:
    """
    Handles deterministic hashing using SHA-1.
    """
    def hash(self, key: str) -> int:
        """
        Returns a large integer hash for a given key string.
        Using SHA-1 and taking the first 8 bytes implies a 64-bit keyspace.
        """
        # Encode string to bytes, hash using SHA-1
        hash_bytes = hashlib.sha1(key.encode('utf-8')).digest()
        # Unpack first 8 bytes as a big-endian unsigned long long (0 -> 2^64 - 1)
        return struct.unpack('>Q', hash_bytes[:8])[0]

# --- 2. Node & HashRing Module ---
class ConsistentHashRing:
    """
    Manages the Hash Ring, Node placement, and Lookups.
    """
    def __init__(self, replicas=100):
        """
        replicas: Number of virtual nodes (vnodes) per physical node.
                  Higher K = better load distribution.
        """
        self.replicas = replicas
        self.hash_func = HashFunction()
        self.ring = {}           # Map: hash_val -> physical_node_name
        self.sorted_keys = []    # Sorted list of hash values for O(log N) search
        self.nodes = set()       # Track physical nodes

    def add_node(self, node: str):
        """
        Adds a physical node and its virtual nodes to the ring.
        """
        self.nodes.add(node)
        for i in range(self.replicas):
            # Create a virtual node key, e.g., "NodeA#0", "NodeA#1"
            vnode_key = f"{node}#{i}"
            hash_val = self.hash_func.hash(vnode_key)
            self.ring[hash_val] = node
            # Insert into sorted list while maintaining order
            bisect.insort(self.sorted_keys, hash_val)

    def remove_node(self, node: str):
        """
        Removes a physical node and all its virtual nodes.
        """
        if node not in self.nodes:
            return
        
        self.nodes.remove(node)
        for i in range(self.replicas):
            vnode_key = f"{node}#{i}"
            hash_val = self.hash_func.hash(vnode_key)
            # Remove from ring map
            if hash_val in self.ring:
                del self.ring[hash_val]
            # Remove from sorted keys (O(N) operation in list, acceptable for infrequent config changes)
            # Note: In production, a balanced BST or red-black tree is preferred for faster removal.
            idx = bisect.bisect_left(self.sorted_keys, hash_val)
            if idx < len(self.sorted_keys) and self.sorted_keys[idx] == hash_val:
                self.sorted_keys.pop(idx)

    def get_node(self, key: str) -> str:
        """
        Given a key, find the responsible physical node.
        Time Complexity: O(log N) where N is total vnodes.
        """
        if not self.ring:
            return None
        
        hash_val = self.hash_func.hash(key)
        
        # Binary search for the first hash on the ring >= key's hash
        idx = bisect.bisect_left(self.sorted_keys, hash_val)
        
        # If we reach the end of the list, wrap around to index 0 (Circular Ring)
        if idx == len(self.sorted_keys):
            idx = 0
            
        return self.ring[self.sorted_keys[idx]]

# --- 3. Rebalancer / Evaluator Module ---
class LoadBalancerSimulator:
    """
    Simulates key distribution and measures rebalancing efficiency.
    """
    def __init__(self, num_keys=100000):
        self.num_keys = num_keys
        # Generate stable mock keys (Key0, Key1, ...)
        self.keys = [f"Key{i}" for i in range(num_keys)]

    def get_distribution(self, ring):
        """
        Calculates how many keys are mapped to each node.
        """
        distribution = defaultdict(int)
        for key in self.keys:
            node = ring.get_node(key)
            distribution[node] += 1
        return distribution

    def calculate_diff(self, dist_before, dist_after):
        """
        Calculates the percentage of keys that moved to a DIFFERENT node.
        """
        # To do this accurately, we must re-map every key and check mapping
        # We cannot just compare counts.
        pass # Helper only, actual logic in run_simulation

    def run_scenario(self):
        print("--- Consistent Hashing Simulation ---")
        
        # 1. Setup Initial Ring
        ring = ConsistentHashRing(replicas=100) # K=100 vnodes
        initial_nodes = ["Node_A", "Node_B", "Node_C"]
        for n in initial_nodes:
            ring.add_node(n)
        
        print(f"1. Initial State: {len(initial_nodes)} Nodes, {ring.replicas} vnodes/node")
        
        # 2. Measure Initial Load
        mapping_before = {}
        dist_before = defaultdict(int)
        for key in self.keys:
            node = ring.get_node(key)
            mapping_before[key] = node
            dist_before[node] += 1
            
        # Print Stats
        print(f"   Total Keys: {self.num_keys}")
        print("   Load Distribution:")
        for node, count in dist_before.items():
            print(f"     {node}: {count} keys ({count/self.num_keys:.2%})")

        # 3. Add a Node (Node_D)
        print("\n2. Adding Node_D...")
        ring.add_node("Node_D")
        
        moved_keys = 0
        dist_after_add = defaultdict(int)
        for key in self.keys:
            new_node = ring.get_node(key)
            dist_after_add[new_node] += 1
            if new_node != mapping_before[key]:
                moved_keys += 1
                
        print("   Load Distribution After Add:")
        for node, count in dist_after_add.items():
            print(f"     {node}: {count} keys ({count/self.num_keys:.2%})")
            
        percent_moved = moved_keys / self.num_keys
        ideal_movement = 1.0 / (len(initial_nodes) + 1) # 1/(N+1)
        print(f"   Keys Moved: {moved_keys} ({percent_moved:.2%})")
        print(f"   Theoretical Ideal Movement: {ideal_movement:.2%}")

        # 4. Remove a Node (Node_A)
        print("\n3. Removing Node_A...")
        # Update mapping baseline to current state (with D included)
        mapping_before_remove = {}
        for key in self.keys:
            mapping_before_remove[key] = ring.get_node(key)
            
        ring.remove_node("Node_A")
        
        moved_keys_rem = 0
        dist_after_rem = defaultdict(int)
        for key in self.keys:
            new_node = ring.get_node(key)
            dist_after_rem[new_node] += 1
            if new_node != mapping_before_remove[key]:
                moved_keys_rem += 1

        print("   Load Distribution After Removal:")
        for node, count in dist_after_rem.items():
            print(f"     {node}: {count} keys ({count/self.num_keys:.2%})")
            
        print(f"   Keys Moved: {moved_keys_rem} ({moved_keys_rem/self.num_keys:.2%})")

# --- 4. Main Execution ---
if __name__ == "__main__":
    sim = LoadBalancerSimulator(num_keys=100_000)
    sim.run_scenario()