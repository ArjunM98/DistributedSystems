package client;

import shared.messages.KVMessage;

public interface KVCommInterface {
    /**
     * Establishes a connection to storage service, i.e., to an arbitrary
     * instance of the storage servers that makes up the storage service.
     *
     * @throws Exception if connection could not be established.
     */
    public void connect() throws Exception;

    /**
     * Disconnects the client from the storage service (i.e., the currently
     * connected server).
     */
    public void disconnect();

    /**
     * Inserts a data record into the storage service.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return a message that confirms the insertion of the tuple or an error.
     * @throws Exception if put command cannot be executed
     *                   (e.g. not connected to any storage server).
     */
    public KVMessage put(String key, String value) throws Exception;

    /**
     * Retrieves a data record for a given key from the storage service.
     *
     * @param key the key that identifies the record.
     * @return the value, which is indexed by the given key.
     * @throws Exception if get command cannot be executed
     *                   (e.g. not connected to any storage server).
     */
    public KVMessage get(String key) throws Exception;
}
