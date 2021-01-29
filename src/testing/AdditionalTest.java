package testing;

import client.KVStore;
import junit.framework.TestCase;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class AdditionalTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
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
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.GET, "key", "value", 1);
        assertEquals(msg.getStatus(), KVMessage.StatusType.GET);
    }

    /**
     * Tests KVProto Message Format - Key
     */
    @Test
    public void testKVProtoGetKey() {
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.GET, "key", "value", 1);
        assertEquals(msg.getKey(), "key");
    }

    /**
     * Tests KVProto Message Format - Value
     */
    @Test
    public void testKVProtoGetValue() {
        KVMessageProto msg = new KVMessageProto(KVMessage.StatusType.GET, "key", "value", 1);
        assertEquals(msg.getValue(), "value");
    }

    /**
     * Tests KVProto Marshalling/Unmarshalling - This is achieved by marshalling and recovering the original text
     */
    @Test
    public void testKVProtoWriteParseStream() throws Exception {
        KVMessageProto msgSend = new KVMessageProto(KVMessage.StatusType.GET, "key", "value", 1);
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
     * Ensure that we're able to get each of the possible message statuses
     */
    @Test
    public void testMessageStatus() throws Exception {
        // Test PUTs
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, kvClient.put("key", "value").getStatus());
        assertEquals(KVMessage.StatusType.PUT_UPDATE, kvClient.put("key", "new value").getStatus());
        // TODO: how to force PUT_ERROR?

        // Test GETs
        assertEquals(KVMessage.StatusType.GET_SUCCESS, kvClient.get("key").getStatus());
        assertEquals(KVMessage.StatusType.GET_ERROR, kvClient.get("nonexistent_key").getStatus());

        // Test client-side failures
        char[] bigKey = new char[KVStore.MAX_KEY_SIZE + 1], bigValue = new char[KVStore.MAX_VALUE_SIZE + 1];
        Arrays.fill(bigKey, 'x');
        Arrays.fill(bigValue, 'x');
        assertEquals(KVMessage.StatusType.FAILED, kvClient.get(String.valueOf(bigKey)).getStatus());
        assertEquals(KVMessage.StatusType.FAILED, kvClient.put(String.valueOf(bigKey), "small value").getStatus());
        assertEquals(KVMessage.StatusType.FAILED, kvClient.put("small_key", String.valueOf(bigValue)).getStatus());

        // Test server-side failures
        // TODO: but how? would require malformed protobuf
    }
}
