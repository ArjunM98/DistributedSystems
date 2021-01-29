package testing;

import app_kvServer.KVServer;
import app_kvServer.storage.KVPartitionedStorage;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;

import junit.framework.TestCase;

import shared.messages.KVMessageProto;
import shared.messages.KVMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

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

//    public void cleanUpServer() {
//        kvServer.clearCache();
//        kvServer.clearStorage();
//    }

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
}
