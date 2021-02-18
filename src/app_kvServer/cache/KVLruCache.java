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
        this.cache = new LinkedHashMap<>(cacheSize, 0.75f /* load factor */, true /* ordering mode (i.e. do GETs count?) */) {
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
}
