package app_kvECS;

import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class HashRangeTransfer {

    private static final Logger logger = Logger.getRootLogger();

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

    public TransferType getTransferType() {
        return this.transferType;
    }

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

        logger.info("SENDING LOCK REQUEST");
        // 1. Lock server I'm sending data to
        ack = getLockingNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.LOCK
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.LOCK_ACK)
            throw new IOException("Unable to acquire lock");

        logger.info("SENDING TRANSFER REQUEST");
        // 2. Ask for transfer port available on deleted server
        ack = getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.TRANSFER_REQ
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK)
            throw new IOException("Unable to acquire port information");
        String availablePort = ack.getValue();

        logger.info("SENDING MOVE REQUEST");
        // 3. Send port information and range to new server
        ack = getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                ECSClient.ECS_NAME,
                KVAdminMessage.AdminStatusType.MOVE_DATA,
                getHashRange(),
                getDestinationNode().getNodeHost() + ":" + availablePort
        ), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() != KVAdminMessage.AdminStatusType.MOVE_DATA_ACK)
            throw new IOException("Unable to initiate transfer procedure");

        logger.info("SENDING TRANSFER BEGIN(S)");
        // 4. Initiate transfer and await progress
        final long timeout = 2;
        final TimeUnit timeUnit = TimeUnit.HOURS;
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);

        List<Callable<Boolean>> tasks = Arrays.asList((() -> {
                    try {
                        KVAdminMessageProto response = getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                                ECSClient.ECS_NAME,
                                KVAdminMessage.AdminStatusType.TRANSFER_BEGIN
                        ), timeout, timeUnit);
                        return getSourceNode().getNodeName().equals(response.getSender())
                                && response.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE;
                    } catch (Exception e) {
                        logger.info("Unable to await source node transfer termination", e);
                        return false;
                    }
                }), (() -> {
                    try {
                        KVAdminMessageProto response = getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                                ECSClient.ECS_NAME,
                                KVAdminMessage.AdminStatusType.TRANSFER_BEGIN
                        ), timeout, timeUnit);
                        return getDestinationNode().getNodeName().equals(response.getSender())
                                && response.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE;
                    } catch (Exception e) {
                        logger.info("Unable to await destination node transfer termination", e);
                        return false;
                    }
                })
        );

        try {
            for (Future<Boolean> result : threadPool.invokeAll(tasks, timeout, timeUnit)) {
                if (!result.get(5000L, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Transfer terminated unsuccessfully");
                }
            }
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
