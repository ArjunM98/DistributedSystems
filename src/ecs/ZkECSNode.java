package ecs;

/**
 * A stateful, zookeeper-aware, ECSNode
 */
public class ZkECSNode extends ECSNode {
    private ServerStatus serverStatus;

    /**
     * See {@link ECSNode#fromConfig(String)}
     */
    public static ZkECSNode fromConfig(String config) {
        final ECSNode original = ECSNode.fromConfig(config);
        return new ZkECSNode(original.getNodeName(), original.getNodeHost(), original.getNodePort());
    }

    /**
     * Construct an ECSNode given explicit construction values
     *
     * @param nodeName human-readable identifier for the node
     * @param nodeHost IP/hostname of the node
     * @param nodePort port on which the node listens for client connections
     */
    public ZkECSNode(String nodeName, String nodeHost, int nodePort) {
        super(nodeName, nodeHost, nodePort);
        this.serverStatus = ServerStatus.OFFLINE;
    }

    /**
     * Copy constructor
     *
     * @param original node to copy
     */
    public ZkECSNode(ZkECSNode original) {
        super(original);
        this.serverStatus = original.serverStatus;
    }

    /**
     * See {@link #serverStatus}
     */
    public ServerStatus getNodeStatus() {
        return serverStatus;
    }

    /**
     * See {@link #serverStatus}
     */
    public void setNodeStatus(ServerStatus status) {
        serverStatus = status;
    }

    /**
     * Server status as viewed by an {@link app_kvECS.ECSClient}
     */
    public enum ServerStatus {
        OFFLINE,    // Server is offline
        INACTIVE,   // SSH start call has been given, but a response has not been received
        STARTING,   // Server is in the processing of starting up
        RUNNING,    // Server is actively accepting read and write requests
        STOPPED,    // Server is not running
        STOPPING    // Server is in the process of stopping
    }
}
