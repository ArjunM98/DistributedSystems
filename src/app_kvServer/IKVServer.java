package app_kvServer;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    public enum State {
        ALIVE,             /* Server is alive, but not started */
        DEAD,              /* Server is not alive */
        STARTED,           /* Server is active and ready to respond */
        STOPPED,           /* Server is alive but not responding to requests */
        LOCKED,            /* Server is write locked */
        UNLOCKED,          /* Server is not locked */
        SENDING_TRANSFER,  /* Server is sending data */
        RECEIVING_TRANSFER /* Server is receiving data */
    };

    /**
     * Get the server state
     * @return  state
     */
    public State getServerState();

    /**
     * Set the server state
     * @param  newState the new state of the server
     */
    public void setServerState(State newState);

    /**
     * Get the name of the server
     * @return  name
     */
    public String getServerName();

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();
}
