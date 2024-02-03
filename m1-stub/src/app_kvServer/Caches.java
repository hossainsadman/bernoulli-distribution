package app_kvServer;

import java.util.*;

public class Caches {
    /**
     * Cache Interface
     */
    public static interface Cache<K, V> {
        public V get(K key); // gets the value of given key in the cache

        public void put(K key, V value); // puts a given KV pair in the cache

        public void remove(K key); // removes a given key in the cache

        public boolean removeAll(); // removes every key in the cache
        
        public int size(); // returns the size of the cache

        public boolean containsKey(K key); // checks if the cache contains a given key

        public Set<String> keySet(); // returns a set of all keys in the cache
        public Set<Map.Entry<K, V>> entrySet(); // returns a set of all kvs in the cache
    }

    /**
     * LRU Cache Implementation
     */
    public static class LRUCache implements Cache<String, String> {
        private final LinkedHashMap<String, String> kvs;
        private final int capacity;

        public LRUCache(final int capacity){
            this.capacity = capacity;
            this.kvs = new LinkedHashMap<String, String>(capacity, 0.75f, true){ // access order = true
                @Override // returns true after removing its eldest entry
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest){
                    if (size() > capacity){ // if size exceeds capacity, remove eldest (least recently accessed)
                        String keyToRemove = eldest.getKey();
                        kvs.remove(keyToRemove);
                        return true;
                    }
                    return false;
                }
            };
        }

        @Override
        public String get(String key){
            return kvs.getOrDefault(key, null);
        }

        @Override
        public void put(String key, String value){
            if (capacity <= 0)
                return; // edge case, if capacity is 0, return
            
            if (value.equals("null")){
                remove(key);
                return;
            }

            kvs.put(key, value); 
        }

        @Override
        public void remove(String key) {
            if (!containsKey(key)) return; // no such key
            kvs.remove(key);
        }

        public boolean removeAll() {
            try {
                kvs.clear();
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        @Override
        public int size(){
            return kvs.size();
        }

        @Override
        public boolean containsKey(String key){
            return kvs.containsKey(key);
        }

        @Override
        public Set<String> keySet() {
            return kvs.keySet();
        }
        
        @Override
        public Set<Map.Entry<String, String>> entrySet() {
            return kvs.entrySet();
        }
    }

    /**
     * LFU Cache Implementation
     */
    public static class LFUCache implements Cache<String, String> {
        private final Map<String, String> kvs;
        private final Map<String, Integer> keyFrequencies;
        private final Map<Integer, LinkedHashSet<String>> frequencyToKeys;
        private int minFrequency;
        private final int capacity;

        public LFUCache(int capacity){
            kvs = new HashMap<>();
            keyFrequencies = new HashMap<>();
            frequencyToKeys = new HashMap<>();
            minFrequency = 0;
            this.capacity = capacity;
        }

        // helper method to increment frequency and adjust keyFrequencies and frequencyToKeys given another get request
        private void incrementFrequency(String key){
            int frequency = keyFrequencies.get(key);
            frequencyToKeys.get(frequency).remove(key);
            if (frequencyToKeys.get(frequency).size() == 0) { // empty now
                frequencyToKeys.remove(frequency);
                if (frequency == minFrequency)
                    ++minFrequency;
            }
            ++frequency;

            keyFrequencies.put(key, frequency);

            if (!frequencyToKeys.containsKey(frequency)) // Update frequencyToKeys mapping if frequency + 1 does not exist
                frequencyToKeys.put(frequency, new LinkedHashSet<String>());
            frequencyToKeys.get(frequency).add(key);

            if (frequency == minFrequency && frequencyToKeys.get(frequency).size() == 1) // if frequency is minimum and current key is the only key with this frequency
                ++minFrequency;
        }
        
        @Override
        public String get(String key){
            if (!containsKey(key)) return null;
                
            incrementFrequency(key);
            printCurrState();
            return kvs.get(key);
        }

        @Override
        public void put(String key, String value){
            if (capacity <= 0)
                return; // safety edge case, if capacity is 0, return
            
            if (value.equals("null")) {
                remove(key);
                return;
            }

            if (containsKey(key)) { // if key already exists
                kvs.put(key, value);
                incrementFrequency(key);
                return;
            }
            
            if (size() >= capacity) { // if size exceeds capacity
                LinkedHashSet<String> leastFrequentKeys = frequencyToKeys.get(minFrequency);
                String leastFrequentKey = leastFrequentKeys.iterator().next();
                remove(leastFrequentKey); 
            }

            kvs.put(key, value);
            keyFrequencies.put(key, 1);

            if (!frequencyToKeys.containsKey(1)) // if there is no other keys with frequency 1, append the current kv pair
                frequencyToKeys.put(1, new LinkedHashSet<String>());
            frequencyToKeys.get(1).add(key);
            minFrequency = 1;

            printCurrState();
        }

        @Override
        public void remove(String key){
            if (!containsKey(key)) // does not exist
                return;

            int frequency = keyFrequencies.get(key);
            kvs.remove(key);
            keyFrequencies.remove(key);
            
            frequencyToKeys.get(frequency).remove(key);
            if (frequencyToKeys.get(frequency).size() == 0) {
                frequencyToKeys.remove(frequency);
            }

            if (frequency == minFrequency)
                minFrequency = frequencyToKeys.isEmpty() ? 0 : Collections.min(frequencyToKeys.keySet());
        }

        public boolean removeAll() {
            try {
                kvs.clear();
                keyFrequencies.clear();
                frequencyToKeys.clear();
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        @Override
        public int size(){
            return kvs.size();
        }

        @Override
        public boolean containsKey(String key){
            return kvs.containsKey(key);
        }

        @Override
        public Set<String> keySet() {
            return kvs.keySet();
        }

        @Override
        public Set<Map.Entry<String, String>> entrySet() {
            return kvs.entrySet();
        }

        private void printCurrState() { // For debugging purposes
            System.out.println("Current minFrequency: " + minFrequency);
            System.out.println("Key Frequencies: ");
            for (Map.Entry<String, Integer> entry : keyFrequencies.entrySet())
                System.out.println("\tKey: " + entry.getKey() + ", Frequency: " + entry.getValue());

            System.out.println("Frequency to Keys Map: ");
            for (Map.Entry<Integer, LinkedHashSet<String>> entry : frequencyToKeys.entrySet())
                System.out.println("\tFrequency: " + entry.getKey() + ", Keys: " + entry.getValue());
        }
    }

    /**
     * FIFO Cache Implementation
     */
    public static class FIFOCache implements Cache<String, String> {
        private final Map<String, String> kvs;
        private final Queue<String> queue;
        private final int capacity;

        public FIFOCache(int capacity){
            kvs = new HashMap<>();
            queue = new LinkedList<>();
            this.capacity = capacity;
        }

        @Override
        public String get(String key) {
            return kvs.getOrDefault(key, null);
        }

        @Override
        public void put(String key, String value) {
            if (capacity <= 0)
                return; // edge case, if capacity is 0, return
            
            if (value.equals("null")) {
                remove(key);
                return;
            }
            
            if (!containsKey(key)){
                if (size() >= capacity){ 
                    String keyOldest = queue.poll(); // remove head (i.e. oldest kv)
                    if (keyOldest != null) {
                        kvs.remove(keyOldest);
                    }
                }
                queue.add(key);
            }
            kvs.put(key, value);
        }

        @Override
        public void remove(String key) {
            if (!containsKey(key))
                return; // no such key

            kvs.remove(key);
            queue.remove(key);
        }
        
        public boolean removeAll(){
            try {
                kvs.clear();
                queue.clear();
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        @Override
        public int size(){
            return kvs.size();
        }

        @Override
        public boolean containsKey(String key){
            return kvs.containsKey(key);
        }

        @Override
        public Set<String> keySet() {
            return kvs.keySet();
        }
        
        @Override
        public Set<Map.Entry<String, String>> entrySet() {
            return kvs.entrySet();
        }
    }
}