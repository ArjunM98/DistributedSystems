package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

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
        String key = "foo";
        String value = "foo";

        kvClient.put(key, value);
        kvClient.put(key, "null");
        kvClient.put(key, "null");
        kvClient.put(key, "null");

        KVMessage response = kvClient.get(key);

        assertEquals("", response.getValue());
    }

    /**
     * Ensure that we only respond to requests of keys in the appropriate size range
     */
    @Test
    public void testMaxKeyError() throws Exception {
        String goodKey = "x".repeat(KVStore.MAX_KEY_SIZE);
        String badKey = goodKey + "x";

        assertNotSame(KVMessage.StatusType.FAILED, kvClient.get(goodKey).getStatus());
        assertEquals(KVMessage.StatusType.FAILED, kvClient.get(badKey).getStatus());
    }

    /**
     * Ensure that we only respond to requests of values in the appropriate size range
     */
    @Test
    public void testMaxValueError() throws Exception {
        String goodValue = "x".repeat(KVStore.MAX_VALUE_SIZE);
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
}
