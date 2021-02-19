package ecs;

import java.math.BigInteger;

public class ECSNode implements IECSNode {
    private final String nodeName, nodeHost;
    private final int nodePort;

    private final BigInteger nodeHash;
    private BigInteger predecessorHash;

    /**
     * Construct an ECSNode according to the example ecs.config file provided on Quercus
     *
     * @param nodeName human-readable identifier for the node
     * @param nodeHost IP/hostname of the node
     * @param nodePort port on which the node listens for client connections
     */
    public ECSNode(String nodeName, String nodeHost, int nodePort) {
        this.nodeName = nodeName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;

        this.nodeHash = ECSHashRing.computeHash(this.getConnectionString());
        this.setPredecessor(this);
    }

    /**
     * Save the hash of the predecessor of this node from the {@link ECSHashRing} to build this node's range
     */
    public void setPredecessor(ECSNode predecessor) {
        this.predecessorHash = predecessor.getNodeHash();
    }

    /**
     * @return the connection string that uniquely identifies this node
     */
    public String getConnectionString() {
        return this.nodeHost + ":" + this.nodePort;
    }

    /**
     * @return {@link #nodeHash}
     */
    public BigInteger getNodeHash() {
        return this.nodeHash;
    }

    /**
     * Checks if a key's hash is within this node's hash range
     *
     * @param key to hash and check against this node's range
     * @return true if this ECS node is responsible for a given Key
     */
    public boolean isResponsibleForKey(String key) {
        return this.isResponsibleForHash(ECSHashRing.computeHash(key));
    }

    /**
     * Checks if an MD5 value is within this node's hash range
     *
     * @param hash to check against this node's range
     * @return true if {@link #predecessorHash} < hash <= {@link #nodeHash}
     */
    private boolean isResponsibleForHash(BigInteger hash) {
        return this.nodeHash.compareTo(hash) >= 0
                && this.predecessorHash.compareTo(hash) < 0;
    }

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    @Override
    public String getNodeHost() {
        return this.nodeHost;
    }

    /**
     * @return the port number of the node (ie 8080)
     */
    @Override
    public int getNodePort() {
        return this.nodePort;
    }

    /**
     * @return array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    @Override
    public String[] getNodeHashRange() {
        return new String[]{this.predecessorHash.toString(16), this.nodeHash.toString(16)};
    }
}
