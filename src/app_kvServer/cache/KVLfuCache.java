package app_kvServer.cache;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

public class KVLfuCache implements IKVCache {
    private static final Logger logger = Logger.getRootLogger();
    private final int cacheSize;

    public KVLfuCache(int cacheSize) {
        this.cacheSize = cacheSize;
        logger.warn("Constructor not implemented");
    }

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        return IKVServer.CacheStrategy.LFU;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inCache(String key) {
        logger.warn("inCache() not implemented");
        return false;
    }

    @Override
    public String getKV(String key) {
        logger.warn("getKV() not implemented");
        return null;
    }

    @Override
    public void putKV(String key, String value) {
        logger.warn("putKV() not implemented");
    }

    @Override
    public void delete(String key) throws Exception {
        logger.warn("deleteKV() not implemented");
    }

    @Override
    public void clearCache() {
        logger.warn("clearCache() not implemented");
    }
}