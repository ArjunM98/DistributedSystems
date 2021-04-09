package client;

import shared.messages.KVMessage;

public interface KVCommInterface {
    /**
     * Establishes a connection to storage service, i.e., to an arbitrary
     * instance of the storage servers that makes up the storage service.
     *
     * @throws Exception if connection could not be established.
     */
    void connect() throws Exception;

    /**
     * Disconnects the client from the storage service (i.e., the currently
     * connected server).
     */
    void disconnect();

    /**
     * Inserts a data record into the storage service.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed
     *                   (e.g. not connected to any storage server).
     */
    KVMessage put(String key, String value) throws Exception;

    /**
     * Updates data record in storage service which matches the keyFilter
     *
     * @param keyFilter - Regular expression to match key to
     * @param valueExp  - Value expression to search for in replacement
     * @param valueRepl - New value in update
     * @return all updated values
     * @throws Exception
     */
    KVMessage putAll(String keyFilter, String valueExp, String valueRepl) throws Exception;

    /**
     * Retrieves a data record for a given key from the storage service.
     *
     * @param key the key that identifies the record.
     * @return the value, which is indexed by the given key.
     * @throws Exception if get command cannot be executed
     *                   (e.g. not connected to any storage server).
     */
    KVMessage get(String key) throws Exception;

    /**
     * Retrieves all data records that match the given key filter
     *
     * @param keyFilter - Regular expression statement
     * @return all KVPairs that match the keyFilter
     * @throws Exception
     */
    KVMessage getAll(String keyFilter) throws Exception;

    /**
     * Deletes all data records that match the given key filter
     *
     * @param keyFilter - Regular expression statement
     * @return all deleted values
     * @throws Exception
     */
    KVMessage deleteAll(String keyFilter) throws Exception;
}
