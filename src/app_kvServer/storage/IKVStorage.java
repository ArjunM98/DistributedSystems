package app_kvServer.storage;

import app_kvServer.KVServerException;
import shared.messages.KVMessage.StatusType;

import java.util.function.Predicate;
import java.util.stream.Stream;

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

    /**
     * Get a stream of all {@link KVPair}s in storage which match a certain criteria.
     * <p>
     * This stream is likely backed by a file handle so callers should remember to call {@link Stream#close()} on the
     * resulting stream after it's been processed, or use within a try-with-resources statement.
     *
     * @return {@link Stream} of {@link KVPair} (ideally lazily populated) or empty stream on error
     */
    public Stream<KVPair> openKvStream(Predicate<KVPair> filter);

    /**
     * Batch deletion of all {@link KVPair}s in storage which match a certain criteria.
     */
    public void deleteIf(Predicate<KVPair> filter);

    /**
     * Container class for a key-value pair
     */
    class KVPair {
        /**
         * According to M1 docs, keys will never have a space in them
         */
        public static final String KV_DELIMITER = " ";

        public final String key, value;

        public KVPair(String key, String value) {
            this.key = key;
            this.value = value;
        }

        /**
         * @return serialized string for KVPair
         */
        public String serialize() {
            return this.key + KV_DELIMITER + this.value;
        }

        /**
         * @param serialized see {@link #serialize()}
         * @return deserialized instance of {@link KVPair} or null on failure
         */
        public static KVPair deserialize(String serialized) {
            final int split = serialized.indexOf(KV_DELIMITER);
            return split < 0 ? null : new KVPair(serialized.substring(0, split), serialized.substring(split + 1));
        }
    }
}
