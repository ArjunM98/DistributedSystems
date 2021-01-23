package testing;

import org.junit.Test;

import junit.framework.TestCase;

import shared.messages.KVMessageProto;
import shared.messages.KVMessage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class AdditionalTest extends TestCase {

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

}
