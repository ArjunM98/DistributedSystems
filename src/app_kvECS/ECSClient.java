package app_kvECS;

import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.Utilities;
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
    public static final String ECS_NAME = "ECS";
    public static final String SERVER_JAR = new File(System.getProperty("user.dir"), "m2-server.jar").toString();
    public static final String PUBLIC_ZK_CONN = Utilities.getHostname() + ":2181";
    private static final Logger logger = Logger.getRootLogger();
    /* Zookeeper Client Instance */
    private final ZooKeeperService zk;
    /* ECS Hashring */
    private final ECSHashRing<ZkECSNode> hashRing;
    /* Holds all available nodes available */
    private Queue<ZkECSNode> ECSNodeRepo;
    /* Temporarily holds information about new servers being added */
    private ECSHashRing<ZkECSNode> newHashRing;

    /**
     * Initializes ECS Structure
     * 1. Initializes ECS Node Repository - All Servers
     * 2. Initializes appropriate root znodes to monitor by the ECS service
     *
     * @param filePath                  - ECSConfig file
     * @param zooKeeperConnectionString - like localhost:2181
     */
    public ECSClient(String filePath, String zooKeeperConnectionString) throws IOException {
        logger.debug("Initializing ECS Server");

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

        logger.debug("ECS Server Initialized");
    }

    public static void main(String[] args) {
        new ECSClientCli(args).run();
    }

    public synchronized void close() throws IOException {
        this.zk.close();
    }

    /**
     * {@link IECSClient#start()}
     *
     * @return true on success, false on failure
     * @throws Exception some meaningful exception on failure
     */
    @Override
    public synchronized boolean start() throws Exception {

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
        boolean transferSuccess = executeTransfers(transferList);
        successfulTransfer = successfulTransfer && transferSuccess;

        // Execute reconciliation strategy
        ECSHashRing<ZkECSNode> tempHashRing = hashRing.deepCopy(ZkECSNode::new);
        for (ZkECSNode server : newHashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.STARTING) {
                tempHashRing.addServer(new ZkECSNode(server));
                boolean reconciliation = addNodeReconciliation(tempHashRing, server);
                successfulTransfer = successfulTransfer && reconciliation;
            }
        }

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
        zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toConfig().getBytes(StandardCharsets.UTF_8));

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
        hashRing.clear();
        hashRing.addAll(newHashRing);
        newHashRing = new ECSHashRing<>();
        return successfulTransfer;
    }

    /**
     * {@link IECSClient#stop()}
     *
     * @return true when startup was successful else false
     */
    @Override
    public synchronized boolean stop() {

        if (hashRing.size() == 0) return false;
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
    public synchronized boolean shutdown() {

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
    public synchronized IECSNode addNode(String cacheStrategy, int cacheSize) {
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
    public synchronized Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {

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
        try {
            boolean responseReceived = awaitNodes(count, 15000 /* 15 second timeout limit */);
            if (!responseReceived) throw new Exception();
        } catch (Exception e) {
            logger.warn("Unable to receive connection update", e);
            // reverse setup stage
            nodesToAdd.stream().map(ZkECSNode.class::cast).forEach(node -> {
                node.setNodeStatus(ServerStatus.INACTIVE);
                ECSNodeRepo.add(node);
            });
            nodesToAdd = null;
            newHashRing = new ECSHashRing<>();
        }

        return nodesToAdd;
    }

    /**
     * Sets up `count` servers with the ECS (in this case Zookeeper)
     *
     * @return array of strings, containing unique names of servers
     */
    @Override
    public synchronized Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
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
    public synchronized boolean awaitNodes(int count, int timeout) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < (long) timeout) {
            boolean nodesResponded = newHashRing.getAllNodes().stream()
                    .map(ZkECSNode::getNodeStatus)
                    .allMatch(status -> status != ServerStatus.INACTIVE);
            if (nodesResponded) {
                logger.debug("All Nodes Responded!");
                return true;
            }
            try {
                Thread.sleep(100 /* Sleep for 0.1 seconds */);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while awaiting nodes");
                break;
            }
        }
        logger.debug("Unable to contact server");
        return false;
    }

    /**
     * {@link IECSClient#removeNodes(Collection)}
     *
     * @param nodeNames - names of server(s) for removal
     * @return true when removal of nodeNames was successful else false
     */
    @Override
    public synchronized boolean removeNodes(Collection<String> nodeNames) {

        boolean successfulStop = true;

        // make a new hash ring copy
        newHashRing = hashRing.deepCopy(ZkECSNode::new);

        // Go around ring and mark prospective clients as stopping
        for (String name : nodeNames) {
            ZkECSNode node = newHashRing.getNodeByName(name);
            node.setNodeStatus(ServerStatus.STOPPING);
        }

        // Replication reconciliation
        ECSHashRing<ZkECSNode> tempHashRing = newHashRing.deepCopy(ZkECSNode::new);
        for (ZkECSNode server : newHashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.STOPPING) {
                boolean reconciliation = removeNodeReconciliation(tempHashRing, server);
                successfulStop = successfulStop && reconciliation;
                tempHashRing.removeServer(server);
            }
        }

        // Shutdown respective servers
        for (ZkECSNode server : newHashRing.getAllNodes()) {
            if (server.getNodeStatus() == ServerStatus.STOPPING) {
                try {
                    KVAdminMessageProto ack = server.sendMessage(zk, new KVAdminMessageProto(
                            ECS_NAME,
                            KVAdminMessage.AdminStatusType.SHUTDOWN
                    ), 10000, TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.SHUTDOWN_ACK) throw new IOException();
                    server.setNodeStatus(ServerStatus.OFFLINE);
                    newHashRing.removeServer(server);
                    ECSNodeRepo.add(server);
                } catch (IOException e) {
                    server.setNodeStatus(ServerStatus.STOPPED);
                    successfulStop = false;
                    logger.warn("Unable to shutdown servers");
                }
            }
        }

        // Update data
        try {
            zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toConfig().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.warn("Unable to update metadata", e);
        }

        // Reset state
        hashRing.clear();
        hashRing.addAll(newHashRing);
        newHashRing = new ECSHashRing<>();

        return successfulStop;
    }

    @Override
    public synchronized Map<String, IECSNode> getNodes() {
        return hashRing.getAllNodes().stream().collect(Collectors.toMap(IECSNode::getNodeName, Function.identity()));
    }

    @Override
    public synchronized IECSNode getNodeByKey(String Key) {
        return hashRing.getServer(Key);
    }

    /**
     * Given a list of TransferPairs, initiate and monitor transfers - follows removeNode or addNode procedure
     *
     * @param transferList - Object representing transfer information between two servers
     */
    private synchronized boolean executeTransfers(List<HashRangeTransfer> transferList) {
        boolean successfulTransfer = true;
        for (HashRangeTransfer transfer : transferList) {
            try {
                transfer.execute(zk);
            } catch (IOException e) {
                successfulTransfer = false;
                // remove the server either being added or deleted
                // leave the original server untouched
                if (transfer.getTransferType() == TransferType.DESTINATION_ADD) {
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
     * Helper function to coordinates reconciliation of replicated data on a node addition
     *
     * @param tempRing current state of hashRing
     * @param newNode  current node being added
     * @return
     */
    private synchronized boolean addNodeReconciliation(ECSHashRing<ZkECSNode> tempRing, ZkECSNode newNode) {

        boolean successfulReconciliation = true;

        // 1. Get replication transfer list
        List<HashRangeTransfer> predecessorReplication = new ArrayList<>();
        List<HashRangeTransfer> successorReplication = new ArrayList<>();
        for (int i = 1; i < 3; i++) {
            ZkECSNode predecessor = tempRing.getNthPredecessor(newNode, i, false);
            ZkECSNode successor = tempRing.getNthSuccessor(newNode, i, false);
            if (predecessor.getNodeName().equals(newNode.getNodeName())) break;
            predecessorReplication.add(new HashRangeTransfer(predecessor, newNode, predecessor.getNodeHashRange(), TransferType.REPLICATION));
            successorReplication.add(new HashRangeTransfer(newNode, successor, successor.getNodeHashRange(), TransferType.REPLICATION));
        }

        // 2. Copy replicated data from previous two predecessors
        for (HashRangeTransfer transfer : predecessorReplication) {
            try {
                logger.debug("transferring from " + transfer.getSourceNode().getNodeName() + " to " + transfer.getDestinationNode().getNodeName());
                transfer.execute(zk);
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to replicate predecessor data onto new server", e);
            }
        }

        // 3. Establish new KVServer connections on predecessors and successors
        boolean newConnectionPred = establishNewKVServerConn(predecessorReplication);
        boolean newConnectionSucc = establishNewKVServerConn(successorReplication);
        boolean deleteConnections = deleteConnections(tempRing, predecessorReplication);
        successfulReconciliation = successfulReconciliation && deleteConnections && newConnectionPred && newConnectionSucc;

        // 4. Release write locks on predecessors
        for (HashRangeTransfer transfer : predecessorReplication) {
            try {
                KVAdminMessageProto ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.UNLOCK
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to complete replication by releasing locks on predecessors of new server");
            }
        }

        // 5. Clean up successor servers by deleting stale data ranges
        boolean deleteStaleData = deleteStaleRanges(tempRing, predecessorReplication);
        successfulReconciliation = successfulReconciliation && deleteStaleData;

        // 6. 3rd successor of newNode needs to delete keyRange of newNode (edge case not handled in
        // deleteConnectionsAndRanges)
        ZkECSNode successor = tempRing.getNthSuccessor(newNode, 3, false);
        if (!successor.getNodeName().equals(newNode.getNodeName())) {
            try {
                successor.sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.DELETE,
                        newNode.getNodeHashRange()
                ), 5000, TimeUnit.MILLISECONDS);
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to delete keyRange");
            }
        }

        return successfulReconciliation;
    }

    /**
     * Helper function to coordinates reconciliation of replicated data on a node removal
     *
     * @param tempRing    current state of hashRing
     * @param removedNode current node being removed
     * @return
     */
    private synchronized boolean removeNodeReconciliation(ECSHashRing<ZkECSNode> tempRing,
                                                          ZkECSNode removedNode) {
        boolean successfulReconciliation = true;

        // 1. Calculate Node Transfers
        List<HashRangeTransfer> replicationTransfers = new ArrayList<>();
        for (int i = 1; i < 3; i++) {
            ZkECSNode predecessor = tempRing.getNthPredecessor(removedNode, i, false);
            ZkECSNode successor = tempRing.getNthSuccessor(removedNode, 3 - i, false);
            if (predecessor.getNodeName().equals(removedNode.getNodeName()) ||
                    predecessor.getNodeName().equals(successor.getNodeName())) break;
            replicationTransfers.add(new HashRangeTransfer(predecessor, successor, predecessor.getNodeHashRange(), TransferType.REPLICATION));
        }

        // 2. Execute Transfers
        for (HashRangeTransfer transfer : replicationTransfers) {
            try {
                logger.debug("transferring from " + transfer.getSourceNode().getNodeName() + " to " + transfer.getDestinationNode().getNodeName());
                transfer.execute(zk);
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to replicate predecessor data onto successors", e);
            }
        }

        // 3. 3rd successor needs to also get the data of the removed node
        ZkECSNode successor = tempRing.getNthSuccessor(removedNode, 3, false);
        if (!successor.getNodeName().equals(removedNode.getNodeName())) {
            try {
                new HashRangeTransfer(removedNode, successor, removedNode.getNodeHashRange(), TransferType.REPLICATION).execute(zk);
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to replicate predecessor data onto successors", e);
            }
        }

        // 4. Assign new appropriate persistent connections
        boolean newConnection = establishNewKVServerConn(replicationTransfers);
        successfulReconciliation = successfulReconciliation && newConnection;

        // 5. Delete hanging stale persistent connections
        boolean deleteConnection = deleteConnections(tempRing, replicationTransfers);
        successfulReconciliation = successfulReconciliation && deleteConnection;

        // 5. Release write locks on predecessors
        for (HashRangeTransfer transfer : replicationTransfers) {
            try {
                KVAdminMessageProto ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.UNLOCK
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                successfulReconciliation = false;
                logger.warn("Unable to complete replication by releasing locks on predecessors of removed server");
            }
        }

        return successfulReconciliation;
    }

    /**
     * Deletes persistent connection between source node and the 3rd successor from source node
     *
     * @param tempRing     hashRing associated with current state
     * @param transferList list of transferObjects
     * @return
     */
    private synchronized boolean deleteConnections(ECSHashRing<ZkECSNode> tempRing,
                                                   List<HashRangeTransfer> transferList) {

        boolean disconnectSuccessful = true;
        for (HashRangeTransfer transfer : transferList) {

            // 1. Find 3rd successor from source node in each transfer object
            ZkECSNode successor = tempRing.getNthSuccessor(transfer.getSourceNode(), 3, false);
            if (successor.getNodeName().equals(transfer.getSourceNode().getNodeName())) continue;

            // 2. Disconnect source node from successor
            try {
                KVAdminMessageProto ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.DISCONNECT_REPLICA,
                        successor.getNodeName()
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.DISCONNECT_REPLICA_ACK) throw new IOException();
            } catch (IOException e) {
                disconnectSuccessful = false;
                logger.warn("Unable to disconnect from stale replicas");
            }

        }
        return disconnectSuccessful;
    }

    /**
     * Deletes key range of source node on the 3rd successor from source node
     *
     * @param tempRing     hashRing associated with current state
     * @param transferList list of transferObjects
     * @return
     */
    private synchronized boolean deleteStaleRanges(ECSHashRing<ZkECSNode> tempRing,
                                                   List<HashRangeTransfer> transferList) {
        boolean deletionSuccessful = true;
        for (HashRangeTransfer transfer : transferList) {

            // 1. Find 3rd successor from source node in each transfer object
            ZkECSNode successor = tempRing.getNthSuccessor(transfer.getSourceNode(), 3, false);
            if (successor.getNodeName().equals(transfer.getSourceNode().getNodeName())) continue;

            // 2. Delete key range from successor
            try {
                logger.debug("Deleting key range of " + transfer.getSourceNode().getNodeName() + " from " + successor.getNodeName());
                KVAdminMessageProto ack = successor.sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.DELETE,
                        transfer.getSourceNode().getNodeHashRange()
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.DELETE_ACK) throw new IOException();
            } catch (IOException e) {
                deletionSuccessful = false;
                logger.warn("Unable to delete stale data from successor servers");
            }
        }
        return deletionSuccessful;
    }

    /**
     * Coordinates establishing a persistent connection between source and destination servers for each
     * transfer object in transferList
     *
     * @param transferList Data transfers that took place between source node and destination node
     * @return
     */
    private synchronized boolean establishNewKVServerConn(List<HashRangeTransfer> transferList) {

        boolean successfulConn = true;

        for (HashRangeTransfer transfer : transferList) {
            try {
                // 1. Ask for an open port number to connect to on destination node
                String availablePort;
                KVAdminMessageProto ack = transfer.getDestinationNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.REPLICA_PORT
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.REPLICA_PORT_ACK) throw new IOException();
                availablePort = ack.getValue();

                // 2. Establish persistent server connection from source node to destination node
                logger.debug("Connect from server " + transfer.getSourceNode().getNodeName() + " to " + transfer.getDestinationNode().getNodeName());
                ack = transfer.getSourceNode().sendMessage(zk, new KVAdminMessageProto(
                        ECS_NAME,
                        KVAdminMessage.AdminStatusType.CONNECT_REPLICA,
                        transfer.getDestinationNode().getNodeHost() + ":" + availablePort
                ), 5000, TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.CONNECT_REPLICA_ACK) throw new IOException();

            } catch (IOException e) {
                successfulConn = false;
                logger.warn("Unable to obtain port information to open a persistent server connection on");
            }

        }
        return successfulConn;
    }

    /**
     * Given a valid newHashRing, the following procedure initializes the transfer data procedure
     * 1. If the node is in state ServerStatus.STARTING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING node to get data from
     * 2. If the node is in state ServerStatus.STOPPING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING to give data to
     */
    private synchronized List<HashRangeTransfer> calculateNodeTransfers() {

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
    private synchronized ZkECSNode getNextValidNode(ZkECSNode node) {

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

        // There was a deletion and that does not concern this function
        synchronized (hashRing) {
            if (children.size() <= hashRing.size()) return;

            // Only handle cases when a there is a potential new server
            if (newHashRing.size() != 0) {

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
                        newServerConnected.setNodeStatus(ServerStatus.STARTING);

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
    public synchronized void handleNodeFailure(ZkECSNode node) {

        logger.debug("Node failure triggered");

        // Check whether the node is currently active:
        ECSNode serverFailed;
        serverFailed = hashRing.getNodeByName(node.getNodeName());

        if (serverFailed != null) {
            logger.info((serverFailed.getNodeName()));
            // need to recover from an active server failure
            node.setNodeStatus(ServerStatus.OFFLINE);
            // remove from the node pool
            hashRing.removeServer(node);
            // add to queue
            ECSNodeRepo.add(node);
            // send metadata update to everyone
            try {
                zk.setData(ZooKeeperService.ZK_METADATA, hashRing.toConfig().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.error(String.format("Unable to send metadata. Due to server failure from: %s", serverFailed.getNodeName()), e);
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
    private synchronized void recoverState(ZkECSNode node) {
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
    private synchronized void invokeKVServerProcess(IECSNode nodeData, String cacheStrategy, int cacheSize) throws IOException {
        String script = String.join(" ",
                "java -jar",
                SERVER_JAR,
                String.valueOf(nodeData.getNodePort()),
                nodeData.getNodeName(),
                PUBLIC_ZK_CONN,
                String.valueOf(cacheSize),
                cacheStrategy);
        script = "ssh -n " + nodeData.getNodeHost() + " nohup " + script + " > server.log &";
        Runtime run = Runtime.getRuntime();
        logger.debug(script);
        run.exec(script);
    }
}
