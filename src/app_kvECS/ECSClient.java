package app_kvECS;

import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import ecs.zkwatcher.ECSMessageResponseWatcher;
import ecs.zkwatcher.ECSRootChange;
import ecs.zkwatcher.ECSServerDeletionWatcher;
import ecs.zkwatcher.ECSTransferWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ECSClient implements IECSClient {
    private static final Logger logger = Logger.getRootLogger();
    public static final String ECS_NAME = "ECS";
    public static final String SERVER_JAR = new File(System.getProperty("user.dir"), "m2-server.jar").toString();

    /* Zookeeper Client Instance */
    private ZooKeeperService zk;

    /* Holds all available nodes available */
    private Queue<ECSNode> ECSNodeRepo;

    /* Temporarily holds information about new servers being added */
    private ECSHashRing newHashRing;

    /* ECS Hashring */
    private ECSHashRing hashRing;

    /**
     * Initializes ECS Structure
     * 1. Initializes ECS Node Repository - All Servers
     * 2. Initializes appropriate root znodes to monitor by the ECS service
     *
     * @param filePath                  - ECSConfig file
     * @param zooKeeperConnectionString - like localhost:2181
     */
    public ECSClient(String filePath, String zooKeeperConnectionString) {
        logger.info("Initializing ECS Server");

        /* Holds original file information passed to ECS on initialization */
        File ecsConfig = new File(filePath);
        hashRing = new ECSHashRing();
        newHashRing = new ECSHashRing();

        // Add all servers to a queue
        try (Stream<String> lines = Files.lines(ecsConfig.toPath())) {
            ECSNodeRepo = lines
                    .map(ECSNode::fromConfig)
                    .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        } catch (IOException e) {
            logger.error("Unable to initialize ECS", e);
        }

        try {
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
            ECSRootChange rootWatcher = new ECSRootChange(this, zk);
            zk.getChildren(ZooKeeperService.ZK_SERVERS, rootWatcher.childrenChanges, rootWatcher.processChangedChildren);

        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error("Unable to setup zookeeper connection", e);
        }

        logger.info("ECS Server Initialized");
    }

    /**
     * {@link IECSClient#start()}
     *
     * @return true on success, false on failure
     * @throws Exception some meaningfull exception on failure
     */
    @Override
    public boolean start() throws Exception {

        // No servers queued up for start
        if (newHashRing.size() == 0) throw new IllegalStateException("No server queued up for start");

        boolean successfulTransfer = true;

        // Send initialization command
        for (IECSNode server : newHashRing.getAllNodes().values()) {
            if (server.getNodeStatus() == IECSNode.ServerStatus.STARTING) {
                try {
                    KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, server).sendMessage(
                            new KVAdminMessageProto(ECS_NAME,
                                    KVAdminMessage.AdminStatusType.INIT,
                                    newHashRing.toConfig()),
                            5000,
                            TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.INIT_ACK) throw new IOException();
                } catch (IOException e) {
                    newHashRing.removeServer((ECSNode) server);
                    logger.warn("Unable to receive response from server for init");
                }
            }
        }

        // Go through servers and calculate data transfers and transfer the appropriate data
        List<TransferPair> transferList = calculateNodeTransfers();
        transferDataAddition(transferList);

        // Send start command to starting servers
        // Update status on successful response
        for (IECSNode server : newHashRing.getAllNodes().values()) {
            if (server.getNodeStatus() == IECSNode.ServerStatus.STARTING) {
                try {
                    KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, server).sendMessage(
                            new KVAdminMessageProto(ECS_NAME,
                                    KVAdminMessage.AdminStatusType.START),
                            5000,
                            TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.START_ACK) throw new IOException();
                    server.setNodeStatus(IECSNode.ServerStatus.RUNNING);
                } catch (IOException e) {
                    newHashRing.removeServer((ECSNode) server);
                    logger.warn("Unable to receive response from server for start");
                }
            }
        }

        // Update data
        zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toString().getBytes(StandardCharsets.UTF_8));

        // Release write lock
        for (TransferPair transfer : transferList) {
            try {
                KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, transfer.getTransferFrom()).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.UNLOCK),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                newHashRing.removeServer((ECSNode) transfer.getTransferFrom());
                logger.warn("Unable to release lock on locked servers");
            }
        }

        if (hashRing.size() < newHashRing.size()) {
            successfulTransfer = false;
        }

        // Reset state
        hashRing = newHashRing;
        newHashRing = new ECSHashRing();
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
        for (IECSNode server : hashRing.getAllNodes().values()) {
            if (server.getNodeStatus() == IECSNode.ServerStatus.RUNNING) {
                try {
                    KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, server).sendMessage(
                            new KVAdminMessageProto(ECS_NAME,
                                    KVAdminMessage.AdminStatusType.STOP),
                            5000,
                            TimeUnit.MILLISECONDS);
                    if (ack.getStatus() != KVAdminMessage.AdminStatusType.STOP_ACK) throw new IOException();
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
        for (IECSNode server : hashRing.getAllNodes().values()) {
            try {
                KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, server).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.SHUTDOWN),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.SHUTDOWN_ACK) throw new IOException();
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
        return addedNodes.size() >= 1 ? (IECSNode) addedNodes.toArray()[0] : null;
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
        newHashRing = ECSHashRing.fromConfig(hashRing.toConfig());

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
            for (IECSNode node : nodesToAdd) {
                node.setNodeStatus(IECSNode.ServerStatus.INACTIVE);
                ECSNodeRepo.add((ECSNode) node);
            }
            nodesToAdd = null;
            newHashRing = new ECSHashRing();
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
            ECSNode newServer = ECSNodeRepo.poll();
            // Mark the server in an inactive state
            newServer.setNodeStatus(IECSNode.ServerStatus.INACTIVE);
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
            boolean nodesResponded = newHashRing.getAllNodes().values().stream()
                    .map(IECSNode::getNodeStatus)
                    .allMatch(status -> status != IECSNode.ServerStatus.INACTIVE);
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

        // make a new hash ring copy
        newHashRing = ECSHashRing.fromConfig(hashRing.toConfig());

        // Go around ring and mark prospective clients as stopping
        for (String name : nodeNames) {
            IECSNode node = newHashRing.getNodeByName(name);
            node.setNodeStatus(IECSNode.ServerStatus.STOPPING);
        }

        // Calculate transfers and watch transfer process
        List<TransferPair> transferList = calculateNodeTransfers();
        transferDataDeletion(transferList);

        // Update data
        try {
            zk.setData(ZooKeeperService.ZK_METADATA, newHashRing.toString().getBytes(StandardCharsets.UTF_8));
        } catch (KeeperException | InterruptedException e) {
            logger.warn("Unable to update metadata", e);
        }

        // Release write lock
        for (TransferPair transfer : transferList) {
            try {
                KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, transfer.getTransferTo()).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.UNLOCK),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.UNLOCK_ACK) throw new IOException();
            } catch (IOException e) {
                newHashRing.removeServer((ECSNode) transfer.getTransferFrom());
                logger.warn("Unable to release lock on locked servers");
            }
        }

        // shutdown respective servers
        for (TransferPair transfer : transferList) {
            try {
                KVAdminMessageProto ack = new ECSMessageResponseWatcher(zk, transfer.getTransferFrom()).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.SHUTDOWN),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.SHUTDOWN_ACK) throw new IOException();
            } catch (IOException e) {
                newHashRing.removeServer((ECSNode) transfer.getTransferFrom());
                logger.warn("Unable to release lock on locked servers");
            }
        }

        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return null;
    }

    /**
     * Given a list of TransferPairs, initiate and monitor transfers - follows removeNode procedure
     *
     * @param transferList - Object representing transfer information between two servers
     */
    private void transferDataDeletion(List<TransferPair> transferList) {

        KVAdminMessageProto ack;
        for (TransferPair transfers : transferList) {
            try {

                IECSNode transferFrom = transfers.getTransferFrom();
                IECSNode transferTo = transfers.getTransferTo();

                // Lock server I'm sending data to
                ack = new ECSMessageResponseWatcher(zk, transferTo).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.LOCK),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.LOCK_ACK)
                    throw new IOException("Unable to acquire lock");

                // Ask for transfer port available on deleted server
                ack = new ECSMessageResponseWatcher(zk, transferTo).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.TRANSFER_REQ),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK)
                    throw new IOException("Unable to acquire port information");
                String availablePort = ack.getValue();

                // Send port information and range to new server
                ack = new ECSMessageResponseWatcher(zk, transferFrom).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.MOVE_DATA,
                                transfers.getTransferRange(),
                                transferTo.getNodeHost() + ":" + availablePort),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.MOVE_DATA_ACK)
                    throw new IOException("Unable to initiate transfer procedure");

                // Initiate transfer monitor
                boolean fin = new ECSTransferWatcher(zk, transferFrom, transferTo).transferListener(2, TimeUnit.HOURS);
                if (!fin) throw new IOException("Unable to complete transfer procedure");

            } catch (IOException e) {
                newHashRing.removeServer((ECSNode) transfers.getTransferFrom());
                newHashRing.removeServer((ECSNode) transfers.getTransferTo());
                logger.warn("Unable to complete transfer for some servers", e);
            }
        }

    }

    /**
     * Given a list of TransferPairs, initiate and monitor transfers - follows addNode procedure
     *
     * @param transferList - Object representing transfer information between two servers
     */
    private void transferDataAddition(List<TransferPair> transferList) {

        KVAdminMessageProto ack;
        for (TransferPair transfers : transferList) {
            try {

                IECSNode transferFrom = transfers.getTransferFrom();
                IECSNode transferTo = transfers.getTransferTo();

                // Lock server I'm getting data from
                ack = new ECSMessageResponseWatcher(zk, transferFrom).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.LOCK),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.LOCK_ACK)
                    throw new IOException("Unable to acquire lock");

                // Ask for transfer port available on new/deleted server
                ack = new ECSMessageResponseWatcher(zk, transferTo).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.TRANSFER_REQ),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK)
                    throw new IOException("Unable to acquire port information");
                String availablePort = ack.getValue();

                // Send port information and range to new server
                ack = new ECSMessageResponseWatcher(zk, transferFrom).sendMessage(
                        new KVAdminMessageProto(ECS_NAME,
                                KVAdminMessage.AdminStatusType.MOVE_DATA,
                                transfers.getTransferRange(),
                                transferTo.getNodeHost() + ":" + availablePort),
                        5000,
                        TimeUnit.MILLISECONDS);
                if (ack.getStatus() != KVAdminMessage.AdminStatusType.MOVE_DATA_ACK)
                    throw new IOException("Unable to initiate transfer procedure");

                // Initiate transfer monitor
                boolean fin = new ECSTransferWatcher(zk, transferFrom, transferTo).transferListener(2, TimeUnit.HOURS);
                if (!fin) throw new IOException("Unable to complete transfer procedure");

            } catch (IOException e) {
                newHashRing.removeServer((ECSNode) transfers.getTransferFrom());
                newHashRing.removeServer((ECSNode) transfers.getTransferTo());
                logger.warn("Unable to complete transfer for some servers", e);
            }
        }
    }

    /**
     * Given a valid newHashRing, the following procedure initializes the transfer data procedure
     * 1. If the node is in state ServerStatus.STARTING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING node to get data from
     * 2. If the node is in state ServerStatus.STOPPING:
     * - Look ahead in HashRing for a currently ServerStatus.RUNNING to give data to
     */
    private List<TransferPair> calculateNodeTransfers() {

        Collection<IECSNode> changedServerState = newHashRing.getAllNodes().values();
        List<TransferPair> transferList = new ArrayList<>();

        for (IECSNode servers : changedServerState) {
            IECSNode successor = getNextValidNode(servers);
            if (servers.getNodeStatus() == IECSNode.ServerStatus.STARTING && successor != null) {
                TransferPair transferPair = new TransferPair(successor, servers, servers.getNodeHashRange());
                transferList.add(transferPair);
            } else if (servers.getNodeStatus() == IECSNode.ServerStatus.STOPPING && successor != null) {
                TransferPair transferPair = new TransferPair(servers, successor, servers.getNodeHashRange());
                transferList.add(transferPair);
            }
        }
        return transferList;
    }

    /**
     * Helper function to find next ServerStatus.RUNNING node. Look at {@link #calculateNodeTransfers}
     *
     * @param node - get the next valid running node from the current position
     */
    private IECSNode getNextValidNode(IECSNode node) {

        IECSNode nextNode = node;
        for (int pos = 0; pos < newHashRing.size(); pos++) {
            nextNode = newHashRing.getSuccessor((ECSNode) nextNode);
            if (nextNode.getNodeStatus() == IECSNode.ServerStatus.RUNNING) {
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
                ECSNode newServerConnected = newHashRing.getNodeByName(serverName);

                // New child has not been processed yet
                if (newServerConnected != null && newServerConnected.getNodeStatus() == IECSNode.ServerStatus.INACTIVE) {

                    // TODO: Concurrent calls to initializeNewServer when multiple servers are starting might be a problem
                    // Update state of server in newHashRing
                    newHashRing.removeServer(newServerConnected);
                    newServerConnected.setNodeStatus(IECSNode.ServerStatus.STARTING);
                    newHashRing.addServer(newServerConnected);

                    // Establish Connection Object
                    ECSServerDeletionWatcher serverWatcher = new ECSServerDeletionWatcher(newServerConnected, zk, this);

                    // Establish watch on new znode
                    try {
                        zk.getData(ZooKeeperService.ZK_SERVERS + "/" + serverName,
                                serverWatcher.changedState);
                    } catch (KeeperException | InterruptedException e) {
                        logger.warn("Unable to set data watch on new znode created by user");
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * TODO: Need to complete
     * Handle node failure, after a successful SSH call.
     * Assumption: Data is lost in a node failure
     * <p>
     * 1. Node shuts down but has not been started
     * 2. Node shuts down and has been active
     *
     * @param node - the following node has lost connection to ZK
     */
    public void handleNodeFailure(IECSNode node) {

        // Check whether the node is currently active:
        ECSNode serverFailed = hashRing.getNodeByName(node.getNodeName());
        if (serverFailed != null) {
            // need to recover from an active server failure
        } else {
            // need to recover from a starting/stopping server failure
        }

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
