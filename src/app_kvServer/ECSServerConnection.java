package app_kvServer;

import app_kvServer.IKVServer.State;
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
    private ECSHashRing<ECSNode> allEcsNodes;

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
                .format("%s %s %d", server.getServerName(), server.getHostname(), server.getPort()), ECSNode::fromConfig);

        zkService.watchDataForever(zNode, this::handleRequest);
        zkService.watchDataForever(ZooKeeperService.ZK_METADATA, this::handleMetadataUpdate);
    }

    public String getMetadata() {
        return this.allEcsNodes.toConfig();
    }

    public ECSNode getEcsNode() {
        return allEcsNodes.getNodeByName(server.getName());
    }

    private void handleMetadataUpdate(byte[] input) {
        allEcsNodes = ECSHashRing.fromConfig(new String(input, StandardCharsets.UTF_8), ECSNode::fromConfig);
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
                case START:
                    handleStart();
                    break;
                case STOP:
                    handleStop();
                    break;
                case SHUTDOWN:
                    handleShutdown();
                    break;
                case LOCK:
                    handleLock();
                    break;
                case UNLOCK:
                    handleUnlock();
                    break;
                case MOVE_DATA:
                    handleMove(req);
                    break;
                case TRANSFER_REQ:
                    handleTransfer();
                    break;
            }
            throw new KVServerException("Bad request type", KVAdminMessage.AdminStatusType.FAILED);
        } catch (KVServerException | IOException e) {
            logger.warn(String.format("Error processing request: %s", e.getMessage()));
        }
    }

    private void handleStart() throws IOException {
        server.setServerState(State.STARTED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.START_ACK).getBytes());
    }

    private void handleStop() throws IOException {
        server.setServerState(State.STOPPED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.STOP_ACK).getBytes());
    }

    private void handleShutdown() throws IOException {
        server.close();
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.SHUTDOWN_ACK).getBytes());
    }

    private void handleLock() throws IOException {
        server.setServerState(State.LOCKED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.LOCK_ACK).getBytes());
    }

    private void handleUnlock() throws IOException {
        server.setServerState(State.UNLOCKED);
        zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.UNLOCK_ACK).getBytes());
    }

    private void handleTransfer() throws IOException {

        ServerSocket socket;
        socket = new ServerSocket(0);

        int portNum = socket.getLocalPort();

        KVAdminMessageProto ack = new ZkECSNode(server.getServerName(), server.getHostname(), server.getPort())
                .sendMessage(zkService, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.MOVE_DATA_ACK, Integer.toString(portNum)), 5000, TimeUnit.MILLISECONDS);

        if (ack.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_BEGIN) {

            try {
                Socket IOSocket = socket.accept();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(IOSocket.getInputStream()))) {
                    server.putAllFromKvStream(in.lines());

                } catch (IOException e) {
                    logger.error("Error occurred during data transfer", e);
                }
                zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE).getBytes());
            } catch (IOException e) {
                logger.error("Error occurred during data receive", e);
            }
        }
    }

    private void handleMove(KVAdminMessageProto req) throws IOException {

        KVAdminMessageProto ack = new ZkECSNode(server.getServerName(), server.getHostname(), server.getPort())
                .sendMessage(zkService, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.MOVE_DATA_ACK), 5000, TimeUnit.MILLISECONDS);
        if (ack.getStatus() == KVAdminMessage.AdminStatusType.TRANSFER_BEGIN) {
            Socket socket;
            String[] fullAddr = req.getAddress().split(":");
            socket = new Socket(fullAddr[0], Integer.parseInt(fullAddr[1]));

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
                s.forEach(out::println);
                zkService.setData(zNode, new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE).getBytes());
            } catch (IOException e) {
                logger.error("Error occurred during data transfer", e);
            }
        }
    }

}
