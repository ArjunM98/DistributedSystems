package testing;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.KVServerException;
import app_kvServer.cache.IKVCache;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.KVPartitionedStorage;
import client.KVStore;
import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;

public class AdditionalTest extends TestCase {

    private KVStore kvClient;
    private KVStore kvClientAddition;


    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        kvClientAddition = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
            kvClientAddition.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    /**
     * Tests KVProto Message Format - Status
     */
    @Test
    public void testKVProtoGetStatus() {
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.PUT, "key", "value", 1);
        assertEquals(msg.getStatus(), KVMessage.StatusType.PUT);
    }

    /**
     * Tests KVProto Message Format - Key
     */
    @Test
    public void testKVProtoGetKey() {
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.PUT, "key", "value", 1);
        assertEquals(msg.getKey(), "key");
    }

    /**
     * Tests KVProto Message Format - Value
     */
    @Test
    public void testKVProtoGetValue() {
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.PUT, "key", "value", 1);
        assertEquals(msg.getValue(), "value");
    }

    /**
     * Tests KVProto Marshalling/Unmarshalling - This is achieved by marshalling and recovering the original text
     */
    @Test
    public void testKVProtoWriteParseStream() throws Exception {
        KVMessageProto msgSend = new KVMessageProto(KVMessage.StatusType.PUT, "key", "value", 1);
        KVMessageProto msgRecv;

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            msgSend.writeMessageTo(out);

            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                msgRecv = new KVMessageProto(in);
            }

        }

        assertEquals(msgSend.getKey(), msgRecv.getKey());
        assertEquals(msgSend.getValue(), msgRecv.getValue());
        assertEquals(msgSend.getStatus(), msgRecv.getStatus());
    }

    /**
     * Tests KVPartitionedStorage where KV pairs have values with spaces
     */
    @Test
    public void testSpacedKVs() throws Exception {
        String key = "spacedKey";
        String value = "spaced Val";

        kvClient.put(key, value);
        KVMessage response = kvClient.get(key);

        assertEquals(value, response.getValue());
    }

    /**
     * Tests KVPartitionedStorage where KV pairs are overwritten
     */
    @Test
    public void testOverwrittenKVs() throws Exception {
        String key = "foo";
        String value = "foo";
        String newValue = "bar";

        kvClient.put(key, value);
        kvClient.put(key, newValue);

        KVMessage response = kvClient.get(key);

        assertEquals(newValue, response.getValue());
    }

    /**
     * Tests KVPartitionedStorage where KV pairs are deleted multiple times
     */
    @Test
    public void testDeletedKVs() throws Exception {
        KVMessage response;
        String key = "testDeletedKVs";
        String value = "foo";

        response = kvClient.put(key, value);
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());

        response = kvClient.put(key, "null");
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, response.getStatus());

        response = kvClient.get(key);
        assertTrue(response.getValue().isEmpty());

        response = kvClient.put(key, "null");
        assertEquals(KVMessage.StatusType.DELETE_ERROR, response.getStatus());

        response = kvClient.get(key);
        assertTrue(response.getValue().isEmpty());
    }

    /**
     * Tests KVPartitionedStorage's batch deletion functionality
     */
    @Test
    public void testBatchDeleteStorage() {
        final KVPartitionedStorage storage = new KVPartitionedStorage(IKVStorage.STORAGE_ROOT_DIRECTORY + "/test");
        storage.clearStorage();

        // Populate storage
        for (int i = 0; i < 100; i++) storage.putKV("key_" + i, "value_" + i);
        for (int i = 0; i < 100; i++) assertTrue(storage.inStorage("key_" + i));

        // Delete even keys
        try {
            storage.deleteIf(kv -> Integer.parseInt(kv.key.substring("key_".length())) % 2 == 0);
        } catch (KVServerException e) {
            e.printStackTrace();
        }

        // Assert only odd keys remaining
        for (int i = 0; i < 100; i++) assertEquals(i % 2 != 0, storage.inStorage("key_" + i));

        storage.clearStorage();
    }

    /**
     * Tests KVPartitionedStorage's batch retrieve functionality
     */
    @Test
    public void testBatchGetStorage() {
        final KVPartitionedStorage storage = new KVPartitionedStorage(IKVStorage.STORAGE_ROOT_DIRECTORY + "/test");
        storage.clearStorage();

        // Populate storage
        for (int i = 0; i < 100; i++) storage.putKV("key_" + i, "value_" + i);

        // Get all KV pairs
        Stream<IKVStorage.KVPair> KVs = storage.openKvStream(kv -> true);


        // Assert stream contains all added keys
        KVs.forEach(KV -> {
            try {
                assertEquals(KV.value, storage.getKV(KV.key));
            } catch (KVServerException e) {
                e.printStackTrace();
            }
        });


        storage.clearStorage();
    }

    /**
     * Ensure that we only respond to requests of keys in the appropriate size range
     */
    @Test
    public void testMaxKeyError() throws Exception {
        String goodKey = "x".repeat(KVMessageProto.MAX_KEY_SIZE);
        String badKey = goodKey + "x";

        assertNotSame(KVMessage.StatusType.FAILED, kvClient.get(goodKey).getStatus());
        assertEquals(KVMessage.StatusType.FAILED, kvClient.get(badKey).getStatus());
    }

    /**
     * Ensure that we only respond to requests of values in the appropriate size range
     */
    @Test
    public void testMaxValueError() throws Exception {
        String goodValue = "x".repeat(KVMessageProto.MAX_VALUE_SIZE);
        String badValue = goodValue + "x";

        assertNotSame(KVMessage.StatusType.FAILED, kvClient.put("goodkey", goodValue).getStatus());
        assertEquals(KVMessage.StatusType.FAILED, kvClient.put("goodkey", badValue).getStatus());
    }

    /**
     * Ensure that we only respond to requests of values in the appropriate size range
     */
    @Test
    public void testMultipleClientAccess() throws Exception {
        String key = "test1Key";
        String value = "test1Val";

        KVMessage resClient1 = kvClient.put(key, value);
        KVMessage resClient2 = kvClientAddition.get(key);

        assertEquals(KVMessage.StatusType.PUT_SUCCESS, resClient1.getStatus());
        assertEquals(value, resClient2.getValue());
    }

    /**
     * Tests FIFO Cache functionality -- no server
     */
    @Test
    public void testFifoCache() {
        final String KEY_PREFIX = "FIFO_Key_", NEW_KEY_PREFIX = "FIFO_New_Key_", VALUE_PREFIX = "Value_", NEW_VALUE_PREFIX = "New_Value_";

        final int TEST_CACHE_SIZE = 100;
        final IKVCache cache = IKVCache.newInstance(CacheStrategy.FIFO, TEST_CACHE_SIZE);

        /* CACHE FUNCTIONALITY TEST*/

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV(KEY_PREFIX + i, VALUE_PREFIX + i);

        // Ensure the cache is working as intended; reverse loop so FIFO and LRU difference are shown
        for (int i = TEST_CACHE_SIZE - 1; i >= 0; i--) assertEquals(VALUE_PREFIX + i, cache.getKV(KEY_PREFIX + i));

        /* SINGLE EVICTION TEST */

        cache.putKV(KEY_PREFIX + TEST_CACHE_SIZE, VALUE_PREFIX + TEST_CACHE_SIZE);
        assertEquals(TEST_CACHE_SIZE, cache.getCacheSize());

        // The new key should be there
        assertEquals(VALUE_PREFIX + TEST_CACHE_SIZE, cache.getKV(KEY_PREFIX + TEST_CACHE_SIZE));

        // The FIFO key should not
        assertFalse(cache.inCache(KEY_PREFIX + 0));

        /* FULL EVICTION TEST */

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV(NEW_KEY_PREFIX + i, NEW_VALUE_PREFIX + i);

        // Ensure old keys are gone
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertNull(cache.getKV(KEY_PREFIX + i));

        // And new keys are in
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertEquals(NEW_VALUE_PREFIX + i, cache.getKV(NEW_KEY_PREFIX + i));
    }

    /**
     * Tests LRU Cache functionality -- no server
     */
    @Test
    public void testLruCache() {
        final String KEY_PREFIX = "LRU_Key_", NEW_KEY_PREFIX = "LRU_New_Key_", VALUE_PREFIX = "Value_", NEW_VALUE_PREFIX = "New_Value_";

        final int TEST_CACHE_SIZE = 100;
        final IKVCache cache = IKVCache.newInstance(CacheStrategy.LRU, TEST_CACHE_SIZE);

        /* CACHE FUNCTIONALITY TEST*/

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV(KEY_PREFIX + i, VALUE_PREFIX + i);

        // Ensure the cache is working as intended; reverse loop so FIFO and LRU difference are shown
        for (int i = TEST_CACHE_SIZE - 1; i >= 0; i--) assertEquals(VALUE_PREFIX + i, cache.getKV(KEY_PREFIX + i));

        /* SINGLE EVICTION TEST */

        cache.putKV(KEY_PREFIX + TEST_CACHE_SIZE, VALUE_PREFIX + TEST_CACHE_SIZE);
        assertEquals(TEST_CACHE_SIZE, cache.getCacheSize());

        // The new key should be there
        assertEquals(VALUE_PREFIX + TEST_CACHE_SIZE, cache.getKV(KEY_PREFIX + TEST_CACHE_SIZE));

        // The LRU key should not
        assertFalse(cache.inCache(KEY_PREFIX + (TEST_CACHE_SIZE - 1)));

        /* FULL EVICTION TEST */

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV(NEW_KEY_PREFIX + i, NEW_VALUE_PREFIX + i);

        // Ensure old keys are gone
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertNull(cache.getKV(KEY_PREFIX + i));

        // And new keys are in
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertEquals(NEW_VALUE_PREFIX + i, cache.getKV(NEW_KEY_PREFIX + i));
    }

    /**
     * Tests LFU Cache functionality -- no server
     */
    @Test
    public void testLfuCache() {
        final String KEY_PREFIX = "LFU_Key_", NEW_KEY_PREFIX = "LFU_New_Key_", VALUE_PREFIX = "Value_", NEW_VALUE_PREFIX = "New_Value_";

        final int TEST_CACHE_SIZE = 100;
        final IKVCache cache = IKVCache.newInstance(CacheStrategy.LFU, TEST_CACHE_SIZE);

        /* CACHE FUNCTIONALITY TEST*/

        // Fill up the cache
        for (int i = 0; i < TEST_CACHE_SIZE; i++) cache.putKV(KEY_PREFIX + i, VALUE_PREFIX + i);

        // Ensure the cache is working as intended; do multiple accesses so the frequencies are different
        // Also go in reverse so we know that FIFO isn't a factor
        for (int i = TEST_CACHE_SIZE - 1; i >= 0; i--) {
            // Loop s.t. most bins have multiple entries e.g. key 0 is accessed 0 times, 1&2 are accessed 1 time, 3&4 are accessed 2 times, ...
            for (int j = 0; j < Math.ceil(i / 2.0); j++) {
                assertEquals(VALUE_PREFIX + i, cache.getKV(KEY_PREFIX + i));
            }
        }

        /* SINGLE EVICTION TEST */

        cache.putKV(KEY_PREFIX + TEST_CACHE_SIZE, VALUE_PREFIX + TEST_CACHE_SIZE);
        assertEquals(TEST_CACHE_SIZE, cache.getCacheSize());

        // The new key should be there
        assertEquals(VALUE_PREFIX + TEST_CACHE_SIZE, cache.getKV(KEY_PREFIX + TEST_CACHE_SIZE));

        // The LFU key should not
        assertFalse(cache.inCache(KEY_PREFIX + 0));

        /* FULL EVICTION TEST */

        // PUT new keys TEST_CACHE_SIZE times each so they're the most frequent
        for (int i = 0; i < TEST_CACHE_SIZE; i++) {
            for (int j = 0; j < TEST_CACHE_SIZE; j++) {
                cache.putKV(NEW_KEY_PREFIX + i, NEW_VALUE_PREFIX + i);
            }
        }

        // Ensure old keys are gone
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertNull(cache.getKV(KEY_PREFIX + i));

        // And new keys are in
        for (int i = 0; i < TEST_CACHE_SIZE; i++) assertEquals(NEW_VALUE_PREFIX + i, cache.getKV(NEW_KEY_PREFIX + i));
    }

    /**
     * See {@link #testOneNodeHashRing()}
     */
    private void oneNodeHashRingTestHelper(ECSHashRing<ECSNode> hashRing, ECSNode first) {
        // Examples inspired by Quercus diagram
        assertEquals("Expected Tuple_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425D0000000000000000", 16)));
        assertEquals("Expected Tuple_2 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425DFFFFFFFFFFFFFFFF", 16)));
        assertEquals("Expected Tuple_3 -> KVServer_1", first, hashRing.getServer(new BigInteger("684CFAA5C6A75BD90000000000000000", 16)));

        // Edge cases test
        assertEquals("Expected KVServer_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425DC30DE0077EA6494D", 16)));
    }

    /**
     * See {@link #testTwoNodeHashRing()}
     */
    private void twoNodeHashRingTestHelper(ECSHashRing<ECSNode> hashRing, ECSNode first, ECSNode second) {
        // Examples inspired by Quercus diagram
        assertEquals("Expected Tuple_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425D0000000000000000", 16)));
        assertEquals("Expected Tuple_2 -> KVServer_2", second, hashRing.getServer(new BigInteger("2B786438D2C6425DFFFFFFFFFFFFFFFF", 16)));
        assertEquals("Expected Tuple_3 -> KVServer_2", second, hashRing.getServer(new BigInteger("684CFAA5C6A75BD90000000000000000", 16)));

        // Edge cases test
        assertEquals("Expected KVServer_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425DC30DE0077EA6494D", 16)));
        assertEquals("Expected KVServer_2 -> KVServer_2", second, hashRing.getServer(new BigInteger("684CFAA5C6A75BD9EDCD06058CA3F4E6", 16)));
        assertEquals("Expected Wraparound test -> KVServer_1", first, hashRing.getServer(new BigInteger("684CFAA5C6A75BD9FFFFFFFFFFFFFFFF", 16)));
    }

    /**
     * See {@link #testMultiNodeHashRing()}
     */
    private void threeNodeHashRingTestHelper(ECSHashRing<ECSNode> hashRing, ECSNode first, ECSNode second, ECSNode third) {
        // Examples inspired by Quercus diagram
        assertEquals("Expected Tuple_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425D0000000000000000", 16)));
        assertEquals("Expected Tuple_2 -> KVServer_2", second, hashRing.getServer(new BigInteger("2B786438D2C6425DFFFFFFFFFFFFFFFF", 16)));
        assertEquals("Expected Tuple_3 -> KVServer_2", second, hashRing.getServer(new BigInteger("684CFAA5C6A75BD90000000000000000", 16)));

        // Edge cases test
        assertEquals("Expected KVServer_1 -> KVServer_1", first, hashRing.getServer(new BigInteger("2B786438D2C6425DC30DE0077EA6494D", 16)));
        assertEquals("Expected KVServer_2 -> KVServer_2", second, hashRing.getServer(new BigInteger("684CFAA5C6A75BD9EDCD06058CA3F4E6", 16)));
        assertEquals("Expected OLD Wraparound test -> KVServer_3", third, hashRing.getServer(new BigInteger("684CFAA5C6A75BD9FFFFFFFFFFFFFFFF", 16)));
        assertEquals("Expected NEW Wraparound test -> KVServer_1", first, hashRing.getServer(new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)));
    }

    /**
     * Tests Hash Ring functionality for a single node -- no server
     */
    @Test
    public void testOneNodeHashRing() {
        final ECSHashRing<ECSNode> hashRing = new ECSHashRing<>();
        final ECSNode first = new ECSNode("KVServer_1", "localhost", 50000);

        hashRing.addServer(first); // 2B786438D2C6425DC30DE0077EA6494D

        oneNodeHashRingTestHelper(hashRing, first);

        // Test 0 node hash ring
        hashRing.removeServer(first);
        assertNull(hashRing.getServer("doesn't matter what this key is, it will return null"));
    }

    /**
     * Tests Hash Ring functionality for two nodes -- no server
     */
    @Test
    public void testTwoNodeHashRing() {
        final ECSHashRing<ECSNode> hashRing = new ECSHashRing<>();
        final ECSNode first = new ECSNode("KVServer_1", "localhost", 50000),
                second = new ECSNode("KVServer_2", "localhost", 50005);

        hashRing.addServer(first); // 2B786438D2C6425DC30DE0077EA6494D
        hashRing.addServer(second); // 684CFAA5C6A75BD9EDCD06058CA3F4E6

        // Test for 2 nodes and then test again as servers die
        twoNodeHashRingTestHelper(hashRing, first, second);
        hashRing.removeServer(second);
        oneNodeHashRingTestHelper(hashRing, first);
    }

    /**
     * Tests Hash Ring functionality for multiple nodes (3 is good enough to represent the general case) -- no server
     */
    @Test
    public void testMultiNodeHashRing() {
        final ECSHashRing<ECSNode> hashRing = new ECSHashRing<>();
        final ECSNode first = new ECSNode("KVServer_1", "localhost", 50000),
                second = new ECSNode("KVServer_2", "localhost", 50005),
                third = new ECSNode("KVServer_3", "localhost", 55555);

        hashRing.addServer(first); // 2B786438D2C6425DC30DE0077EA6494D
        hashRing.addServer(second); // 684CFAA5C6A75BD9EDCD06058CA3F4E6
        hashRing.addServer(third); // D2ED1C9BD26CB54BBD7B8F71203A0654

        // Test for 3 nodes and then test again as servers die
        threeNodeHashRingTestHelper(hashRing, first, second, third);
        hashRing.removeServer(third);
        twoNodeHashRingTestHelper(hashRing, first, second);
        hashRing.removeServer(second);
        oneNodeHashRingTestHelper(hashRing, first);
    }

    /**
     * Tests {@link ECSHashRing} and {@link ECSNode} serialization/deserialization from ecs.config file format
     */
    @Test
    public void testHashRingSerialization() {
        // Simulate an ecs.config file's contents
        final String ecsConfigFileBlob = "" +
                "server1 127.0.0.1 50000\n" +
                "server2 127.0.0.1 50001\n" +
                "server3 127.0.0.1 50002\n" +
                "server4 127.0.0.1 50003\n" +
                "server5 127.0.0.1 50004\n" +
                "server6 127.0.0.1 50005\n" +
                "server7 127.0.0.1 50006\n" +
                "server8 127.0.0.1 50007";

        // Deserialization into ECSHashRing
        final ECSHashRing<ECSNode> hashRing = ECSHashRing.fromConfig(ecsConfigFileBlob, ECSNode::fromConfig);

        // Serialization of ECSHashRing into string
        final String serializedBlob = hashRing.toConfig();

        // Put into sorted arrays for comparison
        final String[] expected = ecsConfigFileBlob.lines().sorted().toArray(String[]::new);
        final String[] actual = serializedBlob.lines().sorted().toArray(String[]::new);
        assertArrayEquals(expected, actual);
    }
}
