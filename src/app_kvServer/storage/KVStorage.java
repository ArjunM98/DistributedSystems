package app_kvServer.storage;

import org.apache.log4j.Logger;

public class KVStorage implements IKVStorage {
    private static final Logger logger = Logger.getRootLogger();

    public KVStorage() {
        // Note: idk what the constructor will require so i'll assume nothing for now
        logger.warn("Constructor not implemented");
    }

    @Override
    public boolean inStorage(String key) {
        logger.warn("inStorage() not implemented");
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        logger.warn("getKV() not implemented");
        return null;
    }

    @Override
    public String putKV(String key, String value) throws Exception {
        logger.warn("putKV() not implemented");
        return null;
    }

    @Override
    public void deleteKV(String key, String value) throws Exception {
        logger.warn("deleteKV() not implemented");
    }

    @Override
    public void clearStorage() {
        logger.warn("clearStorage() not implemented");
    }
}
