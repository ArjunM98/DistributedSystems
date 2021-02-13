package app_kvServer.cache;

import app_kvServer.IKVServer;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Paper: http://dhruvbird.com/lfu.pdf
 * Simple implementation: https://medium.com/algorithm-and-datastructure/4bac0892bdb3
 */
public class KVLfuCache implements IKVCache {
    /**
     * Thread-safety
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Threshold as to when to start evictions
     */
    private final int MAX_CAPACITY;

    /**
     * KV Cache
     */
    private final Map<String, String> cache;

    /**
     * Track the usage (GETs and PUTs) of items
     */
    private final Map<String, Integer> itemFrequencies;

    /**
     * Collections of items by their frequency of use. We'll use this to implement the Least Frequently Used cache
     * eviction procedure.
     */
    private final Map<Integer, Set<String>> frequencyBins;

    /**
     * Keep track of the minimum key of {@link #frequencyBins} to avoid a linear scan of {@link #frequencyBins}'s key
     * set at eviction time when determining the next key to evict
     */
    private int minFrequency;


    public KVLfuCache(int cacheSize) {
        this.MAX_CAPACITY = cacheSize;
        this.cache = new HashMap<>(MAX_CAPACITY);
        this.itemFrequencies = new HashMap<>(MAX_CAPACITY);
        this.frequencyBins = new HashMap<>();
        this.clearCache();
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.LFU;
    }

    @Override
    public int getCacheSize() {
        try {
            lock.readLock().lock();
            return this.cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean inCache(String key) {
        try {
            lock.readLock().lock();
            return this.cache.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String getKV(String key) {
        try {
            lock.writeLock().lock();
            if (!this.cache.containsKey(key)) return null;

            // Update item frequency
            int previousFrequency = this.itemFrequencies.get(key), newFrequency = previousFrequency + 1;
            this.itemFrequencies.put(key, newFrequency);
            if (!this.frequencyBins.containsKey(newFrequency))
                this.frequencyBins.put(newFrequency, new LinkedHashSet<>());
            this.frequencyBins.get(newFrequency).add(key);

            // Remove it from the old frequency pool; if this was the LFU item, update the minFrequency state
            this.frequencyBins.get(previousFrequency).remove(key);
            if (previousFrequency == this.minFrequency && this.frequencyBins.get(previousFrequency).isEmpty()) {
                this.minFrequency += 1;
            }

            // Fulfill request
            return this.cache.get(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putKV(String key, String value) {
        if (this.MAX_CAPACITY <= 0) return;
        try {
            lock.writeLock().lock();

            // Case 1: Update; Case 2: Insert
            if (this.cache.containsKey(key)) {
                // Update
                this.cache.put(key, value);

                // Increment frequency
                this.getKV(key);
            } else {
                // Run cache eviction if this will put us over the top
                if (this.cache.size() >= this.MAX_CAPACITY) {
                    final String keyToEvict = this.frequencyBins.get(this.minFrequency).iterator().next();
                    this.frequencyBins.get(this.minFrequency).remove(keyToEvict);
                    this.itemFrequencies.remove(keyToEvict);
                    this.cache.remove(keyToEvict);
                }

                // Insert
                this.minFrequency = 1;
                this.cache.put(key, value);
                this.itemFrequencies.put(key, 1);
                this.frequencyBins.get(1).add(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) {
        try {
            lock.writeLock().lock();
            if (!this.cache.containsKey(key)) return;

            // Remove key
            final int frequency = this.itemFrequencies.get(key);
            this.itemFrequencies.remove(key);
            this.frequencyBins.get(frequency).remove(key);
            this.cache.remove(key);

            // Remove it from the old frequency pool; if this was the LFU item, update the minFrequency state
            if (frequency == this.minFrequency && this.frequencyBins.get(frequency).isEmpty()) {
                this.minFrequency = this.frequencyBins.keySet().stream().min(Integer::compareTo).orElse(-1);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clearCache() {
        try {
            lock.writeLock().lock();
            this.minFrequency = -1;
            this.cache.clear();
            this.itemFrequencies.clear();
            this.frequencyBins.clear();
            this.frequencyBins.put(1, new LinkedHashSet<>());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static void main(String[] args) {
        final int TEST_CACHE_SIZE = 100;
        final IKVCache cache = new KVLfuCache(TEST_CACHE_SIZE);

        /* CACHE FUNCTIONALITY TEST*/

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV("Key_" + i, "Value_" + i);

        // Ensure the cache is working as intended; do multiple accesses so the frequencies are different
        // Also go in reverse so we know that FIFO isn't a factor
        for (int i = TEST_CACHE_SIZE - 1; i >= 0; i--) {
            // Loop s.t. most bins have multiple entries e.g. key 0 is accessed 0 times, 1&2 are accessed 1 time, 3&4 are accessed 2 times, ...
            for (int j = 0; j < Math.ceil(i / 2.0); j++) {
                final String cacheValue = cache.getKV("Key_" + i), expectedValue = "Value_" + i;
                if (!cacheValue.equals(expectedValue)) {
                    throw new AssertionError("Expected " + expectedValue + " / Got " + cacheValue);
                }
            }
        }

        /* SINGLE EVICTION TEST */

        cache.putKV("Key_" + TEST_CACHE_SIZE, "Value_" + TEST_CACHE_SIZE);

        // The new key should be there
        if (cache.getCacheSize() == TEST_CACHE_SIZE) {
            final String cacheValue = cache.getKV("Key_" + TEST_CACHE_SIZE), expectedValue = "Value_" + TEST_CACHE_SIZE;
            if (!cacheValue.equals(expectedValue)) {
                throw new AssertionError("Expected " + expectedValue + " / Got " + cacheValue);
            }
        } else throw new AssertionError("Expected " + TEST_CACHE_SIZE + " / Got " + cache.getCacheSize());

        // The oldest key should not
        if (cache.inCache("Key_" + 0)) throw new AssertionError("Key_0 should have been deleted");

        /* FULL EVICTION TEST */

        // PUT new keys TEST_CACHE_SIZE times each so they're the most frequent
        for (int i = 0; i < TEST_CACHE_SIZE; i++) {
            for (int j = 0; j < TEST_CACHE_SIZE; j++) {
                cache.putKV("New_Key_" + i, "New_Value_" + i);
            }
        }

        // Ensure old keys are gone
        for (int i = 0; i < TEST_CACHE_SIZE; i++) {
            final String cacheValue = cache.getKV("Key_" + i);
            if (cacheValue != null) {
                throw new AssertionError("Expected null / Got " + cacheValue);
            }
        }

        // And new keys are in
        for (int i = 0; i < TEST_CACHE_SIZE; i++) {
            final String cacheValue = cache.getKV("New_Key_" + i), expectedValue = "New_Value_" + i;
            if (!cacheValue.equals(expectedValue)) {
                throw new AssertionError("Expected " + expectedValue + " / Got " + cacheValue);
            }
        }

        System.out.printf("%s is working%n", cache.getClass());
    }
}
