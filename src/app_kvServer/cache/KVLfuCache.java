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
}
