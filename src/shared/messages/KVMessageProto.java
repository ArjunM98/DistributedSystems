package shared.messages;

import shared.messages.proto.ProtoKVMessage.KVProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class KVMessageProto implements KVMessage {
    public static final long START_MESSAGE_ID = 1, UNKNOWN_MESSAGE_ID = 0;
    public static final String SERVER_ERROR_KEY = "SERVER_ERROR", CLIENT_ERROR_KEY = "CLIENT_ERROR";
    public static final int MAX_KEY_SIZE = 20, MAX_VALUE_SIZE = 120 * 1024;

    private final KVProto msg;

    /**
     * Overloaded constructor for value-less messages
     */
    public KVMessageProto(StatusType status, String key, long id) {
        this(status, key, "", id);
    }

    /**
     * Constructs a KV message format using protobuf builder.
     *
     * @param status - status type of message associated with message types and for identifying errors.
     * @param key    - key associated with this message.
     * @param value  - value associated with this message.
     */
    public KVMessageProto(StatusType status, String key, String value, long id) {
        msg = KVProto.newBuilder()
                .setStatusMsg(status.ordinal())
                .setKeyMsg(key)
                .setValueMsg(value)
                .setIdMsg(id)
                .build();
    }

    /**
     * Reads and parses a message from an input stream.
     *
     * @param in - Input Stream.
     */
    public KVMessageProto(InputStream in) throws IOException, NullPointerException {
        msg = Objects.requireNonNull(KVProto.parseDelimitedFrom(in));
    }

    public StatusType getStatus() {
        return StatusType.values()[msg.getStatusMsg()];
    }

    public String getKey() {
        return msg.getKeyMsg();
    }

    public String getValue() {
        return msg.getValueMsg();
    }

    public long getId() {
        return msg.getIdMsg();
    }

    /**
     * Serializes message and writes it to output stream.
     *
     * @param out - Output Stream.
     */
    public void writeMessageTo(OutputStream out) throws IOException {
        msg.writeDelimitedTo(out);
    }

    /**
     * @return returns a human readable format of KVProto
     */
    @Override
    public String toString() {
        return String.format("%d: %s<%s,%s>", this.getId(), this.getStatus(), this.getKey(), this.getValue());
    }


    public byte[] getByteRepresentation() {
        return msg.toByteArray();
    }
}