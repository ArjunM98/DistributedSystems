package shared.messages;

import com.google.protobuf.InvalidProtocolBufferException;
import shared.messages.proto.ProtoKVAdminMessage;

public class KVAdminMessageProto implements KVAdminMessage {

    private final ProtoKVAdminMessage.KVAdminProto msg;

    public KVAdminMessageProto(String sender, AdminStatusType statusType, String value) {
        msg = ProtoKVAdminMessage.KVAdminProto.newBuilder()
                .setSender(sender)
                .setStatus(statusType.ordinal())
                .setValue(value)
                .build();
    }

    public KVAdminMessageProto(String sender, AdminStatusType statusType, String[] range, String host) {
        msg = ProtoKVAdminMessage.KVAdminProto.newBuilder()
                .setSender(sender)
                .setStatus(statusType.ordinal())
                .setStart(range[0])
                .setEnd(range[0])
                .setAddress(host)
                .build();
    }

    public KVAdminMessageProto(byte[] data) throws InvalidProtocolBufferException {
        msg = ProtoKVAdminMessage.KVAdminProto.parseFrom(data);
    }

    public KVAdminMessageProto(String sender, AdminStatusType statusType) {
        msg = ProtoKVAdminMessage.KVAdminProto.newBuilder()
                .setSender(sender)
                .setStatus(statusType.ordinal())
                .build();
    }

    @Override
    public String getSender() {
        return msg.getSender();
    }

    @Override
    public AdminStatusType getStatus() {
        return AdminStatusType.values()[msg.getStatus()];
    }

    @Override
    public String getValue() {
        return msg.getValue();
    }

    @Override
    public String[] getRange() {
        return new String[]{msg.getStart(), msg.getEnd()};
    }

    @Override
    public String getAddress() {
        return msg.getAddress();
    }

    @Override
    public byte[] getBytes() {
        return msg.toByteArray();
    }

}
