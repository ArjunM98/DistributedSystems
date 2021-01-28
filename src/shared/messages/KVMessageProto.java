package shared.messages;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Objects;

import shared.messages.proto.ProtoKVMessage.KVProto;

public class KVMessageProto implements KVMessage {

    private final KVProto msg;

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

    public long getId() { return msg.getIdMsg(); }

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
    public String getMessageString() {
        return msg.toString();
    }

}