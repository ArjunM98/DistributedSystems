package app_kvServer.cache;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

public class KVFifoCache implements IKVCache {
    private static final Logger logger = Logger.getRootLogger();
    private final int cacheSize;

    public KVFifoCache(int cacheSize) {
        this.cacheSize = cacheSize;
        logger.warn("KVFifoCache not implemented");
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.FIFO;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
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
    public void delete(String key) throws Exception {
    }

    @Override
    public void clearCache() {
    }
}