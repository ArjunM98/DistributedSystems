package app_kvServer.storage;

public interface IKVStorage {
    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Get the value associated with the key
     *
     * @return value associated with key
     * @throws Exception when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     *
     * @return previous value associated with key if updated, null if there was none
     * @throws Exception on failure e.g. when key not in the key range of the server
     */
    public String putKV(String key, String value) throws Exception;

    /**
     * Delete key-value pair from storage
     *
     * @throws Exception on failure e.g. when key not in the key range of the server
     */
    public void deleteKV(String key, String value) throws Exception;

    /**
     * Clear the storage of the server
     */
    public void clearStorage();
}
