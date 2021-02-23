package app_kvECS;

import ecs.IECSNode;

public class TransferPair {

    private final IECSNode transferFrom;
    private final IECSNode transferTo;
    private final String[] transferRange;

    public TransferPair(IECSNode transferFrom, IECSNode transferTo, String[] transferRange) {
        this.transferFrom = transferFrom;
        this.transferTo = transferTo;
        this.transferRange = transferRange;
    }

    public IECSNode getTransferFrom() {
        return this.transferFrom;
    }

    public IECSNode getTransferTo() {
        return this.transferTo;
    }

    public String[] getTransferRange() {
        return this.transferRange;
    }
}
