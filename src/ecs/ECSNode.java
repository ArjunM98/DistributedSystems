package ecs;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

public class ECSNode implements IECSNode {
    private final String nodeName, nodeHost;
    private final int nodePort;

    private final BigInteger nodeHash;
    private BigInteger predecessorHash;

    /**
     * Parse and construct an ECSNode according to the example ecs.config file provided on Quercus
     *
     * @param config string like "server1 localhost 50000"
     * @return constructed ECSNode
     * @throws IllegalArgumentException if line is poorly formatted
     */
    public static ECSNode fromConfig(String config) {
        try {
            final String DELIMITER = " ";
            final List<String> tokens = Arrays.asList(config.split(DELIMITER));
            if (tokens.size() < 3) throw new IllegalArgumentException("Expected 3 tokens");

            return new ECSNode(
                    String.join(DELIMITER, tokens.subList(0, tokens.size() - 2)),
                    tokens.get(tokens.size() - 2),
                    Integer.parseInt(tokens.get(tokens.size() - 1))
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse config from " + config, e);
        }
    }

    /**
     * Serialize this ECSNode into the format provided in ecs.config
     *
     * @return string representation of this node
     */
    public String toConfig() {
        return this.nodeName + " " + this.nodeHost + " " + this.nodePort;
    }

    /**
     * Construct an ECSNode given explicit construction values
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
        switch (this.nodeHash.compareTo(this.predecessorHash)) {
            case 1: // Regular hash ring check
                return (this.nodeHash.compareTo(hash) >= 0 && this.predecessorHash.compareTo(hash) < 0);
            case 0: // Single node hash ring -->
                return (this.nodeHash.equals(this.predecessorHash));
            case -1: // Wraparound case
                return (this.nodeHash.compareTo(hash) >= 0 || this.predecessorHash.compareTo(hash) > 0);
        }

        return false;
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
