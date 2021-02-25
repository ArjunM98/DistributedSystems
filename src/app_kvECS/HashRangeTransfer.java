package app_kvECS;

import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HashRangeTransfer {

    private final ZkECSNode sourceNode, destinationNode;
    private final String[] hashRange;
    private final TransferType transferType;

    public HashRangeTransfer(ZkECSNode sourceNode, ZkECSNode destinationNode, String[] hashRange, TransferType transferType) {
        this.sourceNode = sourceNode;
        this.destinationNode = destinationNode;
        this.hashRange = hashRange;
        this.transferType = transferType;
    }

    public ZkECSNode getSourceNode() {
        return this.sourceNode;
    }

    public ZkECSNode getDestinationNode() {
        return this.destinationNode;
    }

    public String[] getHashRange() {
        return this.hashRange;
    }

    public TransferType getTransferType() { return this.transferType; }

    public ZkECSNode getLockingNode() {
        switch (this.transferType) {
            case SOURCE_REMOVE:
                return this.getDestinationNode();
            case DESTINATION_ADD:
                return this.getSourceNode();
        }
        throw new IllegalStateException("Unknown transfer configuration");
    }

    public void execute(ZooKeeperService zk) throws IOException {
        KVAdminMessageProto ack;

        // 1. Lock server I'm sending data to
        ack = getLockingNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.LOCK
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.LOCK_ACK)
            throw new IOException("Unable to acquire lock");

        // 2. Ask for transfer port available on deleted server
        ack = getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.TRANSFER_REQ
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK)
            throw new IOException("Unable to acquire port information");
        String availablePort = ack.getValue();

        // 3. Send port information and range to new server
        ack = getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.MOVE_DATA,
                getHashRange(),
                getDestinationNode().getNodeHost() + ":" + availablePort
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.MOVE_DATA_ACK)
            throw new IOException("Unable to initiate transfer procedure");

        // 4. Initiate transfer and await progress
        final long timeout = 2;
        final TimeUnit timeUnit = TimeUnit.HOURS;
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        final Future<KVAdminMessageProto> fromFuture = threadPool.submit(() -> getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.TRANSFER_BEGIN
        ), timeout, timeUnit));
        final Future<KVAdminMessageProto> toFuture = threadPool.submit(() -> getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.TRANSFER_BEGIN
        ), timeout, timeUnit));

        try {
            if (threadPool.awaitTermination(timeout, timeUnit)) {
                final KVAdminMessageProto fromResult = fromFuture.get(), toResult = toFuture.get();

                boolean success = getSourceNode().getNodeName().equals(fromResult.getSender()) && fromResult.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE
                        && getDestinationNode().getNodeName().equals(toResult.getSender()) && toResult.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE;

                if (!success) throw new IOException("Transfer terminated unsuccessfully");
            } else throw new IOException("Transfer did not terminate");
        } catch (Exception e) {
            throw new IOException("Unable to complete transfer", e);
        }
    }

    public enum TransferType {
        /**
         * i.e. source node is being removed from the ring
         */
        SOURCE_REMOVE,

        /**
         * i.e. destination node is joining the ring
         */
        DESTINATION_ADD
    }
}
