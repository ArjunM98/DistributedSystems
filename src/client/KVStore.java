package client;

import ecs.ECSHashRing;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KVStore implements KVCommInterface {
    /**
     * MAX_RETRIES: number of times to re-attempt a SERVER_NOT_RESPONSIBLE message, in case metadata is rapidly changing
     * or the desired server is down and the request had to be sent to a backup.
     * <p>
     * RETRY_BACKOFF_MILLIS: after the first try, we'll immediately try again. If that doesn't work, we'll wait
     * RETRY_BACKOFF_MILLIS before trying again, and then 2x that, then 3x that, and so on until MAX_RETRIES have been
     * attempted. This is to give backup servers time to ingest metadata updates and downed servers time to recover.
     */
    private static final int MAX_RETRIES = 3, RETRY_BACKOFF_MILLIS = 3000;

    private static final Logger logger = Logger.getRootLogger();

    private final ECSHashRing<ECSNode> hashRing = new ECSHashRing<>();
    private final Map<String, Socket> serverConnections = new HashMap<>();

    private final AtomicLong msgID = new AtomicLong(KVMessageProto.START_MESSAGE_ID);

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        hashRing.addServer(new ECSNode("server", address, port));
    }

    @Override
    public void connect() throws Exception {
        for (ECSNode node : hashRing.getAllNodes()) getConnection(node);
    }

    /**
     * Get a socket connecting us to this server
     *
     * @param node server to get connection to
     * @return new or pre-existing socket to this server
     * @throws IOException if socket could not be created
     */
    private Socket getConnection(ECSNode node) throws IOException {
        Socket socket = serverConnections.get(node.getConnectionString());
        if (socket != null) {
            logger.info(String.format("Already connected to %s", node.getConnectionString()));
        } else {
            socket = new Socket(node.getNodeHost(), node.getNodePort());
            logger.info(String.format("New Connection established to %s", node.getConnectionString()));
            serverConnections.put(node.getConnectionString(), socket);
        }
        return socket;
    }

    /**
     * Get a socket connecting us to ideally the desired server, or else another node in the ring (i.e. if our
     * desired server is unreachable). If no servers are reachable, fail.
     *
     * @param node preferred server to connect to
     * @return new or pre-existing socket to the KV Service
     * @throws IOException if no servers are reachable
     */
    private Socket getConnectionOrBackup(ECSNode node) throws IOException {
        try {
            return getConnection(node);
        } catch (IOException e1) {
            logger.error("Cannot connect to desired server", e1);
            final Socket backupSocket = hashRing.getAllNodes().stream()
                    .map(e -> {
                        try {
                            return getConnection(e);
                        } catch (IOException e2) {
                            logger.error("Cannot connect to backup server", e2);
                            return null;
                        }
                    }).filter(Objects::nonNull).findAny().orElse(null);
            if (backupSocket == null) throw new IOException("KV Service unreachable");
            return backupSocket;
        }
    }

    @Override
    public void disconnect() {
        List<Map.Entry<String, Socket>> toDisconnect = new ArrayList<>(serverConnections.entrySet());
        toDisconnect.forEach(e -> disconnect(e.getKey(), e.getValue()));
        hashRing.clear();
    }

    private void disconnect(String connectionString, Socket clientSocket) {
        try {
            if (clientSocket != null) {
                clientSocket.close();
                logger.info(String.format("Disconnected from %s", connectionString));
            } else {
                logger.info(String.format("Was not connected to %s", connectionString));
            }
        } catch (IOException e) {
            logger.error(String.format("Error disconnecting from %s", connectionString), e);
        }
        serverConnections.remove(connectionString);
    }

    @Override
    public KVMessage put(String key, String value) throws IOException {
        long messageId = msgID.get();
        for (int iTry = 0; iTry < MAX_RETRIES; iTry++) {
            // 1. Get a server from our pool to contact
            final ECSNode server = hashRing.getServer(key);
            if (server == null) throw new IOException("Not connected to a KVServer");
            final Socket clientSocket = getConnectionOrBackup(server);

            // 2. Make the request
            messageId = msgID.incrementAndGet();
            try {
                new KVMessageProto(KVMessage.StatusType.PUT, validatedKey(key), validatedValue(value), messageId).writeMessageTo(clientSocket.getOutputStream());
                final KVMessageProto response = new KVMessageProto(clientSocket.getInputStream());
                if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    updateMetadata(response.getValue());
                } else return response;
            } catch (IOException e) {
                disconnect(server.getConnectionString(), clientSocket);
                hashRing.removeServer(server);
            } catch (Exception e) {
                return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), messageId);
            }

            // 3. If the request was not satisfied, (potentially) wait before trying again
            logger.debug("Unable to connect to server, rerouting");
            try {
                Thread.sleep(iTry * RETRY_BACKOFF_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Sleep interrupted", e);
            }
        }
        // 4. Could not satisfy request after multiple attempts
        return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, String.format("Exceeded MAX_RETRIES (%d)", MAX_RETRIES), messageId);
    }

    @Override
    public KVMessage get(String key) throws IOException {
        long messageId = msgID.get();
        for (int iTry = 0; iTry < MAX_RETRIES; iTry++) {
            // 1. Get a server from our pool to contact
            final ECSNode server = hashRing.getServer(key);
            if (server == null) throw new IOException("Not connected to a KVServer");
            final Socket clientSocket = getConnectionOrBackup(server);

            // 2. Make the request
            messageId = msgID.incrementAndGet();
            try {
                new KVMessageProto(KVMessage.StatusType.GET, validatedKey(key), messageId).writeMessageTo(clientSocket.getOutputStream());
                final KVMessageProto response = new KVMessageProto(clientSocket.getInputStream());
                if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    updateMetadata(response.getValue());
                } else return response;
            } catch (IOException e) {
                disconnect(server.getConnectionString(), clientSocket);
                hashRing.removeServer(server);
            } catch (Exception e) {
                return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), messageId);
            }

            // 3. If the request was not satisfied, (potentially) wait before trying again
            logger.debug("Unable to connect to server, rerouting");
            try {
                Thread.sleep(iTry * RETRY_BACKOFF_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Sleep interrupted", e);
            }
        }
        // 4. Could not satisfy request after multiple attempts
        return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, String.format("Exceeded MAX_RETRIES (%d)", MAX_RETRIES), messageId);
    }

    private void updateMetadata(String newConfig) throws IOException {
        final ECSHashRing<ECSNode> newRing = ECSHashRing.fromConfig(newConfig, ECSNode::fromConfig);
        final Set<String> newConnectionStrings = newRing.getAllNodes().stream().map(ECSNode::getConnectionString).collect(Collectors.toSet());

        // Disconnect from the servers that are no longer in the hash ring
        final List<Map.Entry<String, Socket>> toDisconnect = serverConnections.entrySet().stream()
                .filter(entry -> !newConnectionStrings.contains(entry.getKey())).collect(Collectors.toList());
        toDisconnect.forEach(e -> disconnect(e.getKey(), e.getValue()));

        hashRing.clear();
        hashRing.addAll(newRing);
        try {
            connect();
        } catch (Exception e) {
            throw new IOException("Unable to connect to new server");
        }
    }

    private String validatedKey(String key) {
        if (key.isEmpty())
            throw new IllegalArgumentException("Key must not be empty");
        if (key.length() > KVMessageProto.MAX_KEY_SIZE)
            throw new IllegalArgumentException(String.format("Max key length is %d Bytes", KVMessageProto.MAX_KEY_SIZE));
        // TODO: should also check for no space but that would fail testing.InteractionTest.testGetUnsetValue()
        return key;
    }

    private String validatedValue(String value) {
        value = value == null ? "null" : value;
        if (value.length() > KVMessageProto.MAX_VALUE_SIZE)
            throw new IllegalArgumentException(String.format("Max value length is %d Bytes", KVMessageProto.MAX_VALUE_SIZE));
        return value;
    }
}
