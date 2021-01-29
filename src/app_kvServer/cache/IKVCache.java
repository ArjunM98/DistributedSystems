package app_kvServer.cache;

import app_kvServer.IKVServer;

public interface IKVCache {
    /**
     * Get the cache strategy of the server
     *
     * @return cache strategy
     */
    public IKVServer.CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     *
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     *
     * @return value associated with key
     */
    public String getKV(String key);

    /**
     * Put the key-value pair into storage
     */
    public void putKV(String key, String value);

    /**
     * Delete key-value pair from cache
     */
    public void delete(String key) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    static IKVCache newInstance(IKVServer.CacheStrategy strategy, int size) {
        switch (strategy) {
            case LRU:
                return new KVLruCache(size);
            case LFU:
                return new KVLfuCache(size);
            case FIFO:
                return new KVFifoCache(size);
            default:
                break;
        }
        return new NoneCache();
    }

    class NoneCache implements IKVCache {
        @Override
        public IKVServer.CacheStrategy getCacheStrategy() {
            return IKVServer.CacheStrategy.None;
        }

        @Override
        public int getCacheSize() {
            return 0;
        }

        @Override
        public boolean inCache(String key) {
            return false;
        }

        @Override
        public String getKV(String key) {
            return null;
        }

        @Override
        public void putKV(String key, String value) {
        }

        @Override
        public void delete(String key) {
        }

        @Override
        public void clearCache() {
        }
    }
}
