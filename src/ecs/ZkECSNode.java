package ecs;

import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A stateful, zookeeper-aware, ECSNode
 */
public class ZkECSNode extends ECSNode {
    private static final Logger logger = Logger.getRootLogger();
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
     * @return the znode this server should be listening on
     */
    public String getZnode() {
        return ZooKeeperService.ZK_SERVERS + "/" + this.getNodeName();
    }

    /**
     * Send a message to this ECS node i.e. KVServer
     *
     * @param zk       - connection to ZooKeeper through which to send the message
     * @param request  - message to send to node VIA its znode
     * @param timeout  - max time to wait for a response
     * @param timeUnit - unit for timeout
     * @return server's response as a {@link KVAdminMessageProto}
     * @throws IOException if could not send or receive a message
     */
    public synchronized KVAdminMessageProto sendMessage(ZooKeeperService zk, KVAdminMessageProto request, long timeout, TimeUnit timeUnit) throws IOException {
        // 0. Prepare sync/async flow
        final String zNode = this.getZnode();
        final CountDownLatch latch = new CountDownLatch(1);

        // 1. Send the message
        try {
            zk.setData(zNode, request.getBytes());
            zk.watchDataOnce(zNode, latch::countDown);
        } catch (Exception e) {
            throw new IOException("Could not send message", e);
        }

        // 2. Wait for the response
        boolean resRecv = false;
        try {
            resRecv = !latch.await(timeout, timeUnit);
        } catch (InterruptedException e) {
            logger.warn("Unable to wait for latch to count down");
        }

        // 3. Extract response
        KVAdminMessageProto res = null;
        try {
            res = new KVAdminMessageProto(zk.getData(zNode));
        } catch (IOException e) {
            logger.warn("Unable to read response", e);
        }

        // 4. Return response
        if (res == null || !resRecv) throw new IOException("Did not receive a response");
        return res;
    }

    /**
     * Send a message to this ECS node i.e. KVServer
     *
     * @param zk         connection to ZooKeeper through which to send the message
     * @param onDeletion callback to run when this node gets deleted
     * @throws IOException if could not send or receive a message
     */
    public void registerOnDeletionListener(ZooKeeperService zk, Runnable onDeletion) throws IOException {
        try {
            zk.watchDeletion(getZnode(), onDeletion);
        } catch (Exception e) {
            throw new IOException("Unable to set deletion watcher", e);
        }
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
