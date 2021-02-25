package app_kvECS;

import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static app_kvECS.HashRangeTransfer.TransferType;
import static ecs.ZkECSNode.ServerStatus;

public class ECSClient implements IECSClient {
    private static final Logger logger = Logger.getRootLogger();
    public static final String ECS_NAME = "ECS";
    public static final String SERVER_JAR = new File(System.getProperty("user.dir"), "m2-server.jar").toString();

    /* Zookeeper Client Instance */
    private final ZooKeeperService zk;

    /* Holds all available nodes available */
    private Queue<ZkECSNode> ECSNodeRepo;

    /* Temporarily holds information about new servers being added */
    private ECSHashRing<ZkECSNode> newHashRing;

    /* ECS Hashring */
    private ECSHashRing<ZkECSNode> hashRing;

    /**
     * Initializes ECS Structure
     * 1. Initializes ECS Node Repository - All Servers
     * 2. Initializes appropriate root znodes to monitor by the ECS service
     *
     * @param filePath                  - ECSConfig file
     * @param zooKeeperConnectionString - like localhost:2181
     */
    public ECSClient(String filePath, String zooKeeperConnectionString) throws IOException {
        logger.info("Initializing ECS Server");

        /* Holds original file information passed to ECS on initialization */
        hashRing = new ECSHashRing<>();
        newHashRing = new ECSHashRing<>();

        // Add all servers to a queue
        try (Stream<String> lines = Files.lines(Path.of(filePath))) {
            final List<ZkECSNode> nodes = lines.map(ZkECSNode::fromConfig).collect(Collectors.toList());
            Collections.shuffle(nodes);
            ECSNodeRepo = new ConcurrentLinkedQueue<>(nodes);
        } catch (IOException e) {
            logger.error("Unable to initialize ECS", e);
        }

        // Establish Connection to Zookeeper Ensemble
        zk = new ZooKeeperService(zooKeeperConnectionString);

        // Establish root nodes
        if (!zk.nodeExists(ZooKeeperService.ZK_SERVERS)) {
            zk.createNode(ZooKeeperService.ZK_SERVERS, new KVAdminMessageProto(ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY), false);
        }
        if (!zk.nodeExists(ZooKeeperService.ZK_METADATA)) {
            zk.createNode(ZooKeeperService.ZK_METADATA, new KVAdminMessageProto(ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY), false);
        }

        // Set up watch on the children of root
        zk.watchChildrenForever(ZooKeeperService.ZK_SERVERS, this::initializeNewServer);

        logger.info("ECS Server Initialized");
    }

    /**
     * {@link IECSClient#start()}
     *
     * @return true on success, false on failure
     * @throws Exception some meaningful exception on failure
     */
    @Override
    public boolean start() throws Exception {

        // No servers queued up for start
        if (newHashRing.size() == 0) throw new IllegalStateException("No server queued up for start");

        boolean successfulTransfer = true;

        // Send initialization command
        for (ZkECSNode server : newHashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.STARTING) {
                try {
                    KVAdminMessageProto ack = server.sendMessage(zk, new KVAdminMessageProto(
                            ECS_NAME,
                            KVAdminMessage.AdminStatusType.INIT,
                            newHashRing.toConfig()
                    ), 5000, TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.INIT_ACK) throw new IOException();
                } catch (IOException e) {
                    // Failed startup and recover state
                    successfulTransfer = false;
                    recoverState(server);
                    logger.warn("Unable to receive response from server for init");
                }
            }
        }

        // Go through servers and calculate data transfers
        final List<HashRangeTransfer> transferList = calculateNodeTransfers();
        // Execute transfers between all applicable servers
        boolean transferExecution = executeTransfers(transferList);
        successfulTransfer = successfulTransfer && transferExecution;

        // Send start command to starting servers
        // Update status on successful response
        for (ZkECSNode server : newHashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.STARTING) {
                try {
                    KVAdminMessageProto ack = server.sendMessage(zk, new KVAdminMessageProto(
                            ECS_NAME,
                            KVAdminMessage.AdminStatusType.START
                    ), 5000, TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.START_ACK) throw new IOException();
                    server.setNodeStatus(ServerStatus.RUNNING);
                } catch (IOException e) {
                    // Failed startup and recover state
                    successfulTransfer = false;
                    recoverState(server);
                    logger.warn("Unable to receive response from server for start");
                }
            }
        }

        // Update data
        zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toString().getBytes(StandardCharsets.UTF_8));

        // Release write lock
        for (HashRangeTransfer transfer : transferList) {
            try {
                KVAdminMessageProto ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.UNLOCK
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                // Failed startup
                successfulTransfer = false;
                logger.warn("Unable to release lock on locked servers");
            }
        }

        // Reset state
        hashRing = newHashRing;
        newHashRing = new ECSHashRing<>();
        return successfulTransfer;
    }

    /**
     * {@link IECSClient#stop()}
     *
     * @return true when startup was successful else false
     */
    @Override
    public boolean stop() {

        if (hashRing.size() == 0) throw new IllegalStateException("No servers running currently");

        boolean stopSuccessful = true;

        // Send stop command
        for (ZkECSNode server : hashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.RUNNING) {
                try {
                    KVAdminMessageProto ack = server.sendMessage(zk, new KVAdminMessageProto(
                            ECS_NAME,
                            KVAdminMessage.AdminStatusType.STOP
                    ), 5000, TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.STOP_ACK) throw new IOException();
                    server.setNodeStatus(ServerStatus.STOPPED);
                } catch (IOException e) {
                    stopSuccessful = false;
                    logger.warn("Unable to stop all servers");
                }
            }
        }
        return stopSuccessful;
    }

    /**
     * {@link IECSClient#shutdown()}
     *
     * @return true when shutdown was successful else false
     */
    @Override
    public boolean shutdown() {
        if (hashRing.size() == 0) return true;

        boolean successfulShutdown = true;

        // Send shutdown command
        for (ZkECSNode server : hashRing.getAllNodes()) {
            try {
                KVAdminMessageProto ack = server.sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.SHUTDOWN
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.SHUTDOWN_ACK) throw new IOException();
                server.setNodeStatus(ServerStatus.OFFLINE);
                hashRing.removeServer(server);
                ECSNodeRepo.add(server);
            } catch (IOException e) {
                successfulShutdown = false;
                logger.warn("Unable to stop all servers");
            }
        }
        return successfulShutdown;
    }

    /**
     * Create a new KVServer with the specified cache size and replacement strategy and add it to the storage service at an arbitrary position.
     *
     * @return name of new server
     */
    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        Collection<IECSNode> addedNodes = addNodes(1 /* Number of Nodes to Add */,
                cacheStrategy,
                cacheSize);
        return addedNodes != null ? (IECSNode) addedNodes.toArray()[0] : null;
    }

    /**
     * Randomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine.
     * This call launches the storage server with the specified cache size and replacement strategy. For simplicity, locate the KVServer.jar in the
     * same directory as the ECS. All storage servers are initialized with the metadata and any persisted data, and remain in state stopped.
     * NOTE: Must call setupNodes before the SSH calls to start the servers and must call awaitNodes before returning
     *
     * @return set of strings containing the names of the nodes
     */
    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {

        // Initialize new hash ring to hold the position of the new nodes
        newHashRing = hashRing.deepCopy(ZkECSNode::new);

        // Set up nodes
        Collection<IECSNode> nodesToAdd = setupNodes(count, cacheStrategy, cacheSize);

        try {
            for (IECSNode node : nodesToAdd) {
                // Invoke SSH call to each new server
                invokeKVServerProcess(node, cacheStrategy, cacheSize);
            }
        } catch (IOException e) {
            logger.error("Unable to start server(s)", e);
            // reverse setup stage
            nodesToAdd.stream().map(ZkECSNode.class::cast).forEach(node -> {
                node.setNodeStatus(ServerStatus.INACTIVE);
                ECSNodeRepo.add(node);
            });
            nodesToAdd = null;
            newHashRing = new ECSHashRing<>();
        }

        // wait for response on the newly added nodes
        boolean responseReceived = false;
        try {
            responseReceived = awaitNodes(count, 10000 /* 10 second timeout limit */);
        } catch (Exception e) {
            logger.error("Unable to receive connection update", e);
        }

        return responseReceived ? nodesToAdd : null;
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     *
     * @return array of strings, containing unique names of servers
     */
    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> nodesToAdd = new ArrayList<>();

        for (int numAdded = 0; numAdded < count && !ECSNodeRepo.isEmpty(); numAdded++) {
            // Get new server to add
            ZkECSNode newServer = ECSNodeRepo.poll();
            // Mark the server in an inactive state
            newServer.setNodeStatus(ServerStatus.INACTIVE);
            // Add the new server to the new hash ring
            newHashRing.addServer(newServer);
            // Add to list of nodes in queue to be added
            nodesToAdd.add(newServer);
        }

        return nodesToAdd;
    }

    /**
     * {@link IECSClient#awaitNodes(int, int)}
     *
     * @param count   - Number of servers to await a response from
     * @param timeout - Maximum time limit to wait for server response
     * @return - returns true on successfully hearing back from count servers
     */
    @Override
    public boolean awaitNodes(int count, int timeout) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < (long) timeout) {
            boolean nodesResponded = newHashRing.getAllNodes().stream()
                    .map(ZkECSNode::getNodeStatus)
                    .allMatch(status -> status != ServerStatus.INACTIVE);
            if (nodesResponded) {
                logger.info("All Nodes Responded!");
                return true;
            }
            try {
                Thread.sleep(100 /* Sleep for 0.1 seconds */);
            } catch (InterruptedException e) {
                logger.info("Interrupted while awaiting nodes");
                break;
            }
        }
        logger.info("Unable to respond!!!");
        return false;
    }

    /**
     * {@link IECSClient#removeNodes(Collection)}
     *
     * @param nodeNames - names of server(s) for removal
     * @return true when removal of nodeNames was successful else false
     */
    @Override
    public boolean removeNodes(Collection<String> nodeNames) {

        boolean successfulStop = true;

        // make a new hash ring copy
        newHashRing = hashRing.deepCopy(ZkECSNode::new);

        // Go around ring and mark prospective clients as stopping
        for (String name : nodeNames) {
            ZkECSNode node = newHashRing.getNodeByName(name);
            node.setNodeStatus(ServerStatus.STOPPING);
        }

        // Calculate transfers and watch transfer process
        final List<HashRangeTransfer> transferList = calculateNodeTransfers();
        boolean transferExecution = executeTransfers(transferList);
        successfulStop = successfulStop && transferExecution;

        // Update data
        try {
            zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.warn("Unable to update metadata", e);
        }

        // Release write lock
        for (HashRangeTransfer transfer : transferList) {
            try {
                KVAdminMessageProto ack = transfer.getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.UNLOCK
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                successfulStop = false;
                logger.warn("Unable to release lock on locked servers");
            }
        }

        // shutdown respective servers
        for (HashRangeTransfer transfer : transferList) {
            try {
                KVAdminMessageProto ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.SHUTDOWN
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.SHUTDOWN_ACK) throw new IOException();
                transfer.getSourceNode().setNodeStatus(ServerStatus.OFFLINE);
                newHashRing.removeServer(transfer.getSourceNode());
                ECSNodeRepo.add(transfer.getSourceNode());
            } catch (IOException e) {
                transfer.getSourceNode().setNodeStatus(ServerStatus.STOPPED);
                successfulStop = false;
                logger.warn("Unable to release lock on locked servers");
            }
        }

        hashRing = newHashRing;
        newHashRing = new ECSHashRing<>();

        return successfulStop;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return hashRing.getAllNodes().stream().collect(Collectors.toMap(IECSNode::getNodeName, Function.identity()));
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return hashRing.getServer(Key);
    }

    /**
     * Given a list of TransferPairs, initiate and monitor transfers - follows removeNode or addNode procedure
     *
     * @param transferList - Object representing transfer information between two servers
     */
    private boolean executeTransfers(List<HashRangeTransfer> transferList) {
        boolean successfulTransfer = true;
        for (HashRangeTransfer transfer : transferList) {
            try {
                transfer.execute(zk);
            } catch (IOException e) {
                successfulTransfer = false;
                // remove the server either being added or deleted
                // leave the original server untouched
                if(transfer.getTransferType() == TransferType.DESTINATION_ADD) {
                    recoverState(transfer.getDestinationNode());
                } else {
                    recoverState(transfer.getSourceNode());
                }
                logger.warn("Unable to complete transfer for some servers", e);
            }
        }
        return successfulTransfer;
    }

    /**
     * Given a valid newHashRing, the following procedure initializes the transfer data procedure
     * 1. If the node is in state ServerStatus.STARTING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING node to get data from
     * 2. If the node is in state ServerStatus.STOPPING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING to give data to
     */
    private List<HashRangeTransfer> calculateNodeTransfers() {

        List<ZkECSNode> changedServerState = newHashRing.getAllNodes();
        List<HashRangeTransfer> transferList = new ArrayList<>();

        for (ZkECSNode node : changedServerState) {
            ZkECSNode successor = getNextValidNode(node);
            if (node.getNodeStatus() == ServerStatus.STARTING && successor != null) {
                transferList.add(new HashRangeTransfer(successor, node, node.getNodeHashRange(), TransferType.DESTINATION_ADD));
            } else if (node.getNodeStatus() == ServerStatus.STOPPING && successor != null) {
                transferList.add(new HashRangeTransfer(node, successor, node.getNodeHashRange(), TransferType.SOURCE_REMOVE));
            }
        }
        return transferList;
    }

    /**
     * Helper function to find next ServerStatus.RUNNING node. Look at {@link #calculateNodeTransfers}
     *
     * @param node - get the next valid running node from the current position
     */
    private ZkECSNode getNextValidNode(ZkECSNode node) {

        ZkECSNode nextNode = node;
        for (int pos = 0; pos < newHashRing.size(); pos++) {
            nextNode = newHashRing.getSuccessor(nextNode);
            if (nextNode.getNodeStatus() == ServerStatus.RUNNING) {
                return nextNode;
            }
        }
        return null;
    }

    /**
     * Process new children added after an SSH call
     *
     * @param children - new children received from root callback
     */
    public void initializeNewServer(List<String> children) {

        // Only handle cases when a there is a potential new server
        if (children.size() > hashRing.size() && newHashRing.size() != 0) {

            Set<String> newChildren = children.stream()
                    .map(name -> name.substring(name.lastIndexOf("/") + 1))
                    .collect(Collectors.toCollection(HashSet::new));

            // For each new child:
            // 1. Set a watch to handle node failures
            // 2. Establish connection to receive incoming communication
            for (String serverName : newChildren) {
                ZkECSNode newServerConnected = newHashRing.getNodeByName(serverName);

                // New child has not been processed yet
                if (newServerConnected != null && newServerConnected.getNodeStatus() == ServerStatus.INACTIVE) {

                    // Update state of server in newHashRing
                    newHashRing.removeServer(newServerConnected);
                    newServerConnected.setNodeStatus(ServerStatus.STARTING);
                    newHashRing.addServer(newServerConnected);

                    // Establish watch on new znode
                    try {
                        newServerConnected.registerOnDeletionListener(zk, () -> handleNodeFailure(newServerConnected));
                    } catch (IOException e) {
                        logger.warn("Unable to set data watch on new znode created by user");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Handle node failure, after a successful SSH call.
     * Assumption: Data is lost in a node failure
     * <p>
     * 1. Node shuts down but has not been started
     * 2. Node shuts down and has been active
     *
     * @param node - the following node has lost connection to ZK
     */
    public void handleNodeFailure(ZkECSNode node) {

        // Check whether the node is currently active:
        ECSNode serverFailed = hashRing.getNodeByName(node.getNodeName());
        if (serverFailed != null) {
            // need to recover from an active server failure
            node.setNodeStatus(ServerStatus.OFFLINE);
            // remove from the node pool
            hashRing.removeServer(node);
            // add to queue
            ECSNodeRepo.add(node);
            // send metadata update to everyone
            try {
                zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toString().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.error("Unable to send metadata", e);
            }
        } else {
            // need to recover from a starting/stopping server failure
        }

    }

    /**
     * Helper function to recover current state from a failed node
     *
     * @param node - failed node response
     */
    private void recoverState(ZkECSNode node) {
        if (newHashRing.size() == 0) throw new IllegalStateException("No server queued up for start");

        // Set the state to offline
        node.setNodeStatus(ServerStatus.OFFLINE);
        // Remove the node from the current new configuration
        newHashRing.removeServer(node);
        // Add the server back to the original repository
        ECSNodeRepo.add(node);
    }

    /**
     * Invokes a remote ssh process to start server at specified host & port
     */
    private void invokeKVServerProcess(IECSNode nodeData, String cacheStrategy, int cacheSize) throws IOException {
        String script = String.join(" ",
                "java -jar",
                SERVER_JAR,
                String.valueOf(nodeData.getNodePort()),
                nodeData.getNodeName(),
                cacheStrategy,
                String.valueOf(cacheSize));
        script = "ssh -n " + nodeData.getNodeHost() + " nohup " + script + " &";
        Runtime run = Runtime.getRuntime();
        run.exec(script);
    }

    public static void main(String[] args) {
        new ECSClientCli(args).run();
    }
}
