package app_kvServer.cache;

import app_kvServer.IKVServer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVLruCache implements IKVCache {
    private final Map<String, String> cache;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public KVLruCache(int cacheSize) {
        this.cache = new LinkedHashMap<>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.LRU;
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
            lock.readLock().lock();
            return this.cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putKV(String key, String value) {
        try {
            lock.writeLock().lock();
            this.cache.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) {
        try {
            lock.writeLock().lock();
            this.cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clearCache() {
        try {
            lock.writeLock().lock();
            this.cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static void main(String[] args) {
        final int TEST_CACHE_SIZE = 100;
        final IKVCache cache = new KVLruCache(TEST_CACHE_SIZE);

        /* CACHE FUNCTIONALITY TEST*/

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV("Key_" + i, "Value_" + i);

        // Ensure the cache is working as intended; reverse loop so FIFO and LRU difference are shown
        for (int i = TEST_CACHE_SIZE - 1; i >= 0; i--) {
            final String cacheValue = cache.getKV("Key_" + i), expectedValue = "Value_" + i;
            if (!cacheValue.equals(expectedValue)) {
                throw new AssertionError("Expected " + expectedValue + " / Got " + cacheValue);
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

        // The LRU key should not
        if (cache.inCache("Key_" + (TEST_CACHE_SIZE - 1))) {
            throw new AssertionError(String.format("Key_%s should have been deleted", (TEST_CACHE_SIZE - 1)));
        }

        /* FULL EVICTION TEST */

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV("New_Key_" + i, "New_Value_" + i);

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
