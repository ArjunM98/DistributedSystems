package app_kvECS;

import ecs.ZkECSNode;

public class TransferPair {

    private final ZkECSNode transferFrom;
    private final ZkECSNode transferTo;
    private final String[] transferRange;
    private final TransferType transferType;

    public TransferPair(ZkECSNode transferFrom, ZkECSNode transferTo, String[] transferRange, TransferType transferType) {
        this.transferFrom = transferFrom;
        this.transferTo = transferTo;
        this.transferRange = transferRange;
        this.transferType = transferType;
    }

    public ZkECSNode getSourceNode() {
        return this.transferFrom;
    }

    public ZkECSNode getDestinationNode() {
        return this.transferTo;
    }

    public String[] getTransferHashRange() {
        return this.transferRange;
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
