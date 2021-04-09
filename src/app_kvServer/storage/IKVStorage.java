package app_kvServer.storage;

import app_kvServer.KVServerException;
import shared.messages.KVMessage.StatusType;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface IKVStorage {
    String STORAGE_ROOT_DIRECTORY = "data";

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     *
     * @return true if key in storage, false otherwise
     */
    boolean inStorage(String key);

    /**
     * Get the value associated with the key
     *
     * @return value associated with key
     * @throws KVServerException e.g. for {@link StatusType#GET_ERROR}, {@link StatusType#FAILED}
     */
    String getKV(String key) throws KVServerException;

    /**
     * Gets all values associated with the regular expression
     *
     * @return all values associated with regular expression
     * @throws KVServerException e.g. for {@link StatusType#GET_ERROR}, {@link StatusType#FAILED}
     */
    List<KVPair> getAllKV(Predicate<KVPair> filter) throws KVServerException;

    /**
     * Put the key-value pair into storage
     *
     * @throws KVServerException e.g. for {@link StatusType#PUT_ERROR}, {@link StatusType#FAILED}
     */
    void putKV(String key, String value) throws KVServerException;

    /**
     * Put all key-value pair(s) into storage
     */
    List<KVPair> putAllKV(Predicate<KVPair> filter, String valExpr, String valRepl);

    /**
     * Delete key-value pair from storage
     *
     * @throws KVServerException e.g. for {@link StatusType#DELETE_ERROR}, {@link StatusType#FAILED}
     */
    void delete(String key) throws KVServerException;

    /**
     * Clear the storage of the server
     */
    void clearStorage();

    /**
     * Get a stream of all {@link KVPair}s in storage which match a certain criteria.
     * <p>
     * This stream is likely backed by a file handle so callers should remember to call {@link Stream#close()} on the
     * resulting stream after it's been processed, or use within a try-with-resources statement.
     *
     * @return {@link Stream} of {@link KVPair} (ideally lazily populated) or empty stream on error
     */
    Stream<KVPair> openKvStream(Predicate<KVPair> filter);

    /**
     * Batch deletion of all {@link KVPair}s in storage which match a certain criteria.
     */
    void deleteIf(Predicate<KVPair> filter) throws KVServerException;

    /**
     * Container class for a key-value pair
     */
    class KVPair {
        /**
         * According to M1 docs, keys will never have a space in them
         */
        public static final String KV_DELIMITER = " ";

        public final Tombstone tombstone;
        public final String key, value;

        public KVPair(String key, String value) {
            this(Tombstone.VALID, key, value);
        }

        public KVPair(Tombstone tombstone, String key, String value) {
            this.tombstone = tombstone;
            this.key = key;
            this.value = value;
        }

        /**
         * @param serialized see {@link #serialize()}
         * @return deserialized instance of {@link KVPair} or null on failure
         */
        public static KVPair deserialize(String serialized) {
            if (serialized == null) return null;
            final int split = serialized.indexOf(KV_DELIMITER);
            return split < 0 ? null : new KVPair(
                    Tombstone.fromChar(serialized.charAt(0)),
                    serialized.substring(1, split),
                    serialized.substring(split + 1)
            );
        }

        /**
         * @return serialized string for KVPair
         */
        public String serialize() {
            return this.tombstone.marker + this.key + KV_DELIMITER + this.value;
        }

        public enum Tombstone {
            VALID('V'),
            DEAD('D');

            final char marker;

            Tombstone(char marker) {
                this.marker = marker;
            }

            static Tombstone fromChar(char marker) {
                return marker == VALID.marker ? VALID : DEAD;
            }
        }
    }
}
