import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

// --- 1. HashFunction Module ---
class HashFunction {
    private final MessageDigest md;

    public HashFunction() {
        try {
            // Using SHA-1 for better distribution than MD5, deterministic output
            this.md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 algorithm not found", e);
        }
    }

    /**
     * Returns a 64-bit deterministic hash for a given key.
     * Maps the key to the ring space: Long.MIN_VALUE to Long.MAX_VALUE
     */
    public long hash(String key) {
        md.reset();
        md.update(key.getBytes());
        byte[] digest = md.digest();
        // Use the first 8 bytes to create a long
        return ByteBuffer.wrap(digest).getLong();
    }
}

// --- 2. Node Module ---
class Node {
    private final String name;
    
    public Node(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Generates virtual node keys (e.g., "NodeA#0", "NodeA#1")
     */
    public List<String> getVNodeKeys(int replicas) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < replicas; i++) {
            keys.add(name + "#" + i);
        }
        return keys;
    }

    @Override
    public String toString() {
        return name;
    }
}

// --- 3. HashRing Module ---
class ConsistentHashRing {
    private final int replicas;
    private final HashFunction hashFunction;
    // TreeMap provides O(log N) lookup and automatic sorting
    private final TreeMap<Long, Node> ring; 

    public ConsistentHashRing(int replicas) {
        this.replicas = replicas;
        this.hashFunction = new HashFunction();
        this.ring = new TreeMap<>();
    }

    public void addNode(String nodeName) {
        Node node = new Node(nodeName);
        for (String vNodeKey : node.getVNodeKeys(replicas)) {
            long hash = hashFunction.hash(vNodeKey);
            ring.put(hash, node);
        }
    }

    public void removeNode(String nodeName) {
        // Iterator avoids ConcurrentModificationException during removal
        Iterator<Map.Entry<Long, Node>> it = ring.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Node> entry = it.next();
            if (entry.getValue().getName().equals(nodeName)) {
                it.remove();
            }
        }
    }

    /**
     * Core Consistent Hashing Logic
     * Given a key, find the first node clockwise on the ring.
     */
    public String getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        long hash = hashFunction.hash(key);

        // tailMap(hash) returns all keys >= hash
        // logic: find first key >= hash. If none, wrap around to first key.
        Long targetHash = ring.ceilingKey(hash);
        
        if (targetHash == null) {
            // Wrap around: Key is larger than the largest node hash, so it goes to the first node
            targetHash = ring.firstKey();
        }

        return ring.get(targetHash).getName();
    }
}

// --- 4. Rebalancer & Simulation Module ---
public class ConsistentHashing {

    private static final int NUM_KEYS = 100_000;
    private static final int VNODES = 100;

    public static void main(String[] args) {
        System.out.println("=== Consistent Hashing Simulation (Java) ===\n");

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < NUM_KEYS; i++) {
            keys.add("user_sess_" + i);
        }

        // 1. Initial State
        ConsistentHashRing ring = new ConsistentHashRing(VNODES);
        List<String> initialNodes = Arrays.asList("Node_A", "Node_B", "Node_C");
        
        System.out.println("1. Initializing Ring with 3 Nodes (" + VNODES + " vnodes each)...");
        for (String node : initialNodes) {
            ring.addNode(node);
        }

        // Baseline Mapping
        Map<String, String> mappingBefore = getMapping(ring, keys);
        printDistribution(mappingBefore);

        // 2. Add Node Scenario
        System.out.println("\n2. [SCENARIO] Adding 'Node_D'...");
        ring.addNode("Node_D");
        
        Map<String, String> mappingAfterAdd = getMapping(ring, keys);
        logMovement(mappingBefore, mappingAfterAdd);
        printDistribution(mappingAfterAdd);

        // 3. Remove Node Scenario
        System.out.println("\n3. [SCENARIO] Removing 'Node_A'...");
        ring.removeNode("Node_A");
        
        Map<String, String> mappingAfterRemove = getMapping(ring, keys);
        logMovement(mappingAfterAdd, mappingAfterRemove);
        printDistribution(mappingAfterRemove);
    }

    // --- Helper Methods for Metrics ---

    private static Map<String, String> getMapping(ConsistentHashRing ring, List<String> keys) {
        Map<String, String> mapping = new HashMap<>();
        for (String key : keys) {
            mapping.put(key, ring.getNode(key));
        }
        return mapping;
    }

    private static void printDistribution(Map<String, String> mapping) {
        Map<String, Integer> dist = new HashMap<>();
        for (String node : mapping.values()) {
            dist.put(node, dist.getOrDefault(node, 0) + 1);
        }

        System.out.println("   Load Distribution:");
        
        // Calculate Mean and StdDev
        double mean = (double) mapping.size() / dist.size();
        double sumSquaredDiff = 0.0;
        
        for (Map.Entry<String, Integer> entry : dist.entrySet()) {
            double percent = (entry.getValue() / (double) mapping.size()) * 100;
            System.out.printf("     %s: %d keys (%.2f%%)\n", entry.getKey(), entry.getValue(), percent);
            sumSquaredDiff += Math.pow(entry.getValue() - mean, 2);
        }
        
        double stdDev = Math.sqrt(sumSquaredDiff / dist.size());
        System.out.printf("   Stats: Mean=%.2f, StdDev=%.2f\n", mean, stdDev);
    }

    private static void logMovement(Map<String, String> oldMap, Map<String, String> newMap) {
        int moved = 0;
        Map<String, Integer> flows = new HashMap<>();
        List<String> sampleLogs = new ArrayList<>();

        for (Map.Entry<String, String> entry : oldMap.entrySet()) {
            String key = entry.getKey();
            String oldNode = entry.getValue();
            String newNode = newMap.get(key);

            if (!oldNode.equals(newNode)) {
                moved++;
                String flow = oldNode + " -> " + newNode;
                flows.put(flow, flows.getOrDefault(flow, 0) + 1);
                
                if (sampleLogs.size() < 3) {
                    sampleLogs.add("   [Log] Key '" + key + "' moved: " + flow);
                }
            }
        }

        System.out.println("   Key Movement Analysis:");
        for (String log : sampleLogs) {
            System.out.println(log);
        }
        
        System.out.println("   Movement Summary:");
        for (Map.Entry<String, Integer> entry : flows.entrySet()) {
            System.out.println("     " + entry.getKey() + ": " + entry.getValue() + " keys");
        }

        double percent = (moved / (double) oldMap.size()) * 100;
        System.out.printf("   Total Moved: %d (%.2f%%)\n", moved, percent);
    }
}