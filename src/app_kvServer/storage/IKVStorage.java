package app_kvServer.storage;

import app_kvServer.KVServerException;
import shared.messages.KVMessage.StatusType;

public interface IKVStorage {
    public static final String STORAGE_ROOT_DIRECTORY = "data";

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
     * @throws KVServerException e.g. for {@link StatusType#GET_ERROR}, {@link StatusType#FAILED}
     */
    public String getKV(String key) throws KVServerException;

    /**
     * Put the key-value pair into storage
     *
     * @throws KVServerException e.g. for {@link StatusType#PUT_ERROR}, {@link StatusType#FAILED}
     */
    public void putKV(String key, String value) throws KVServerException;

    /**
     * Delete key-value pair from storage
     *
     * @throws KVServerException e.g. for {@link StatusType#DELETE_ERROR}, {@link StatusType#FAILED}
     */
    public void delete(String key) throws KVServerException;

    /**
     * Clear the storage of the server
     */
    public void clearStorage();
}
