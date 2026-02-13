import bisect #sorted ring
import hashlib #hash generator

class ConsistentHashRing:
    def __init__(self, num_virtual_nodes=100):
        self.num_virtual_nodes = num_virtual_nodes
        self.ring = {} #empty dictionary hash_position → node_name
        self.sorted_keys = [] # sorted list
        self.nodes = set() #Stores physical nodes
        
    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(),16)
    
    def add_node(self, node_name):
        if node_name in self.nodes:
            return
        
        self.nodes.add(node_name)
        
        for i in range(self.num_virtual_nodes):
            virtual_key = f"{node_name}#{i}"
            
            position = self._hash(virtual_key)
            
            self.ring[position] = node_name
            bisect.insort(self.sorted_keys, position)
            
    def get_node(self, key):
        if not self.ring:
            return None
        
        key_pos = self._hash(key)
        
        idx = bisect.bisect_right(self.sorted_keys, key_pos)
        
        if idx == len(self.sorted_keys):
            idx = 0
            
        node_position = self.sorted_keys[idx]
        
        return self.ring[node_position]
    
    def remove_node(self, node_name):
        
        if node_name not in self.nodes:
            return
        
        self.nodes.remove(node_name)
        
        for i in range(self.num_virtual_nodes):
            virtual_key = f"{node_name}#{i}"
            position = self._hash(virtual_key)
            
            if position in self.ring:
                del self.ring[position]
                
            idx = bisect.bisect_left(self.sorted_keys, position)
            
            if idx < len(self.sorted_keys) and self.sorted_keys[idx] == position:
                self.sorted_keys.pop(idx)
                
            
class StorageService:
    def __init__(self):
        self.ring = ConsistentHashRing()
        # This acts as the actual hard drives for the nodes
        self.storage = {
            "Node_A": {}, 
            "Node_B": {}, 
            "Node_C": {}
        }

    def write(self, key, value):
        # 1. Ask Ring: "Where does this go?"
        node = self.ring.get_node(key)
        # 2. Save data to that node's dictionary
        self.storage[node][key] = value

    def remove_node_safe(self, node_to_remove):
        """
        Gracefully removes a node and MIGRATES its data.
        """
        print(f"⚠️ Removing {node_to_remove}...")
        
        # 1. Get all data currently on this node
        data_to_move = self.storage[node_to_remove]
        
        # 2. Remove node from the Ring (Topology Change)
        self.ring.remove_node(node_to_remove)
        
        # 3. Re-Distribute the data
        for key, value in data_to_move.items():
            # Ask the Ring: "Who is the NEW owner?"
            new_node = self.ring.get_node(key)
            
            # Copy data to the new node
            if new_node not in self.storage:
                self.storage[new_node] = {}
            self.storage[new_node][key] = value
            
            print(f"  -> Moved '{key}' from {node_to_remove} to {new_node}")
            
        # 4. Delete the old node's storage
        del self.storage[node_to_remove]
            