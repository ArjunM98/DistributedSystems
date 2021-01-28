package app_kvServer.cache;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

public class KVFifoCache implements IKVCache {
    private static final Logger logger = Logger.getRootLogger();

    @Override
    public IKVServer.CacheStrategy getCacheStrategy() {
        logger.warn("getCacheStrategy() not implemented");
        return null;
    }

    @Override
    public int getCacheSize() {
        logger.warn("getCacheSize() not implemented");
        return 0;
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
    public void deleteKV(String key, String value) throws Exception {
        logger.warn("deleteKV() not implemented");
    }

    @Override
    public void clearCache() {
        logger.warn("clearCache() not implemented");
    }
}
