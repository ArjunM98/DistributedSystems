package app_kvServer;

import app_kvServer.storage.IKVStorage;
import com.google.protobuf.InvalidProtocolBufferException;
import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ECSServerConnection {
    private static final Logger logger = Logger.getRootLogger();

    private final KVServer server;
    private final ZooKeeperService zkService;
    private final String zNode;

    private ECSHashRing<ZkECSNode> allEcsNodes;
    private CountDownLatch transferLatch;

    /**
     * Constructs a new ECS Server Connection object for a given node.
     *
     * @param server    the server for which to maintain the connection.
     * @param zkService the ZooKeeperService instance to manipulate nodes.
     */
    public ECSServerConnection(KVServer server, ZooKeeperService zkService) {
        this.server = server;
        this.zkService = zkService;
        this.zNode = ZooKeeperService.ZK_SERVERS + "/" + server.getServerName();

        try {
            zkService.createNode(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.EMPTY), true);
        } catch (IOException e) {
            logger.error("Error creating node", e);
        }

        allEcsNodes = ECSHashRing.fromConfig(String
                .format("%s %s %d", server.getServerName(), server.getHostname(), server.getPort()), ZkECSNode::fromConfig);

        zkService.watchDataForever(zNode, this::handleRequest);
        zkService.watchDataForever(ZooKeeperService.ZK_METADATA, this::handleMetadataUpdate);
    }

    public String getMetadata() {
        return this.allEcsNodes.toConfig();
    }

    public ECSNode getEcsNode() {
        return allEcsNodes.getNodeByName(server.getServerName());
    }

    private void handleMetadataUpdate(byte[] input) {
        logger.info("Handling Metadata Update");
        allEcsNodes = ECSHashRing.fromConfig(new String(input, StandardCharsets.UTF_8), ZkECSNode::fromConfig);
    }

    /**
     * Parent method to handle receiving a KVAdminMessage.
     */
    private void handleRequest(byte[] input) {
        KVAdminMessageProto req;
        try {
            try {
                req = new KVAdminMessageProto(input);
            } catch (InvalidProtocolBufferException e) {
                throw new KVServerException("Malformed request", KVAdminMessage.AdminStatusType.FAILED);
            }

            //prevent feedback loop
            if (req.getSender().equals(server.getServerName())) {
                return;
            }

            logger.debug("Responding to request on " + server.getPort());
            switch (req.getStatus()) {
                case INIT:
                    handleInit(req);
                    return;
                case START:
                    handleStart();
                    return;
                case STOP:
                    handleStop();
                    return;
                case SHUTDOWN:
                    handleShutdown();
                    return;
                case LOCK:
                    handleLock();
                    return;
                case UNLOCK:
                    handleUnlock();
                    return;
                case MOVE_DATA:
                    handleMove(req);
                    return;
                case TRANSFER_REQ:
                    handleTransfer();
                    return;
                case TRANSFER_BEGIN:
                    logger.info("RECEIVED TRANSFER BEGIN");
                    handleTransferBegin();
                    return;
                default:
                    logger.info(req.getSender());
                    logger.info(server.getServerName());
                    throw new KVServerException(String.format("Bad request type: %s", req.getStatus()), KVAdminMessage.AdminStatusType.FAILED);
            }
        } catch (KVServerException | IOException e) {
            logger.warn(String.format("Error processing request: %s", e.getMessage()));
        }
    }

    private void handleInit(KVAdminMessageProto req) throws IOException {
        server.setServerState(State.STOPPED);
        allEcsNodes = ECSHashRing.fromConfig(req.getValue(), ZkECSNode::fromConfig);
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.INIT_ACK).getBytes());
    }

    private void handleStart() throws IOException {
        server.setServerState(State.STARTED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.START_ACK).getBytes());
    }

    private void handleStop() throws IOException {
        server.setServerState(State.STOPPED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.STOP_ACK).getBytes());
    }

    private void handleShutdown() throws IOException {
        server.setServerState(State.STOPPED);
        server.clearStorage();
        server.close();
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.SHUTDOWN_ACK).getBytes());
    }

    private void handleLock() throws IOException {
        server.setServerState(State.LOCKED);
        logger.info("SENDING LOCK ACK");
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.LOCK_ACK).getBytes());
    }

    private void handleUnlock() throws IOException {
        server.setServerState(State.UNLOCKED);
        logger.info("SENDING UNLOCK ACK");
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.UNLOCK_ACK).getBytes());
    }

    private void handleTransfer() throws IOException {
        ServerSocket socket = new ServerSocket(0);

        int portNum = socket.getLocalPort();
        logger.info("HANDLING TRANSFER");
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK, Integer.toString(portNum)).getBytes());
        logger.info("SENT TRANSFER ACK BACK");
        Executors.newCachedThreadPool().execute(() -> {
            boolean beginReceived = false;
            transferLatch = new CountDownLatch(1);
            try {
                beginReceived = transferLatch.await(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Unable to wait for latch to count down");
            }
//        KVAdminMessageProto ack = ((ZkECSNode) getEcsNode())
//                .sendMessage(zkService, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK, Integer.toString(portNum)), 15000, TimeUnit.MILLISECONDS);
//        logger.info(ack.getStatus().toString());

            if (beginReceived) {
                logger.info("HANDLE TRANSFER GOT TRANSFER BEGIN");
                try {
                    Socket IOSocket = socket.accept();
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(IOSocket.getInputStream()))) {
                        logger.info("RECEIVED DATA");
                        server.putAllFromKvStream(in.lines());
                        logger.info("SENDING TRANSFER COMPLETE");
                        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE).getBytes());
                    } catch (IOException e) {
                        logger.error("Error occurred during data transfer", e);
                    }
                } catch (IOException e) {
                    logger.error("Error occurred during data receive", e);
                }
            }
        });
    }

    private void handleMove(KVAdminMessageProto req) throws IOException {
        logger.info("HANDLING MOVE");
        zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.MOVE_DATA_ACK).getBytes());
        logger.info("SENT MOVE ACK BACK");
        Executors.newCachedThreadPool().execute(() -> {
            transferLatch = new CountDownLatch(1);
            boolean beginReceived = false;
            try {
                beginReceived = transferLatch.await(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Unable to wait for latch to count down");
            }
//        KVAdminMessageProto ack = ((ZkECSNode) getEcsNode())
//                .sendMessage(zkService, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.MOVE_DATA_ACK), 15000, TimeUnit.MILLISECONDS);
//        logger.info(ack.getStatus().toString());
            if (beginReceived) {
                logger.info("HANDLE MOVE GOT TRANSFER BEGIN");
                Socket socket = null;
                String[] fullAddr = req.getAddress().split(":");
                try {
                    socket = new Socket(fullAddr[0], Integer.parseInt(fullAddr[1]));
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String[] range = req.getRange();
                BigInteger l = new BigInteger(range[0], 16);
                BigInteger r = new BigInteger(range[1], 16);

                Predicate<IKVStorage.KVPair> filter = null;
                switch (r.compareTo(l)) {
                    case 0: // Single node hash ring: this node is responsible for everything
                        filter = kvPair -> true;
                        break;
                    case 1: // Regular hash ring check: (node >= hash > predecessor)
                        filter = kvPair -> {
                            BigInteger hash = ECSHashRing.computeHash(kvPair.key);
                            return (r.compareTo(hash) >= 0 && l.compareTo(hash) < 0);
                        };
                        break;
                    case -1: // Wraparound case: either (node >= hash) OR (hash > predecessor)
                        filter = kvPair -> {
                            BigInteger hash = ECSHashRing.computeHash(kvPair.key);
                            return (r.compareTo(hash) >= 0 || l.compareTo(hash) < 0);
                        };
                        break;
                }

                try (Stream<String> s = server.openKvStream(filter);
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    logger.info("SENDING DATA");
                    s.forEach(out::println);
                    logger.info("SENDING TRANSFER COMPLETE");
                    zkService.setData(zNode, new KVAdminMessageProto(server.getServerName(), KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE).getBytes());
                } catch (IOException e) {
                    logger.error("Error occurred during data transfer", e);
                }
            }
        });
    }

    private void handleTransferBegin() {
        transferLatch.countDown();
    }

    public enum State {
        ALIVE,             /* Server is alive, but not started */
        DEAD,              /* Server is not alive */
        STARTED,           /* Server is active and ready to respond */
        STOPPED,           /* Server is alive but not responding to requests */
        LOCKED,            /* Server is write locked */
        UNLOCKED,          /* Server is not locked */
        SENDING_TRANSFER,  /* Server is sending data */
        RECEIVING_TRANSFER /* Server is receiving data */
    }
}