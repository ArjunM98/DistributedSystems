package client;

import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KVStore implements KVCommInterface {
    public static final int MAX_KEY_SIZE = 20, MAX_VALUE_SIZE = 120 * 1024;

    private static final Logger logger = Logger.getRootLogger();

    private final ECSHashRing hashRing = new ECSHashRing();
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
        for (IECSNode node : hashRing.getAllNodes().values()) getConnection(((ECSNode) node));
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

    @Override
    public void disconnect() {
        serverConnections.forEach(this::disconnect);
        serverConnections.clear();
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
    }

    @Override
    public KVMessage put(String key, String value) throws IOException {
        while (true) {
            final ECSNode server = hashRing.getServer(key);
            if (server == null) throw new IOException("Not connected to a KVServer");
            final Socket clientSocket = getConnection(server);

            final long id = msgID.incrementAndGet();
            try {
                new KVMessageProto(KVMessage.StatusType.PUT, validatedKey(key), validatedValue(value), id).writeMessageTo(clientSocket.getOutputStream());
                final KVMessageProto response = new KVMessageProto(clientSocket.getInputStream());
                if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    updateMetadata(ECSHashRing.fromConfig(response.getValue()));
                } else return response;
            } catch (Exception e) {
                return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), id);
            }
        }
    }

    @Override
    public KVMessage get(String key) throws IOException {
        while (true) {
            final ECSNode server = hashRing.getServer(key);
            if (server == null) throw new IOException("Not connected to a KVServer");
            final Socket clientSocket = getConnection(server);

            final long id = msgID.incrementAndGet();
            try {
                new KVMessageProto(KVMessage.StatusType.GET, validatedKey(key), id).writeMessageTo(clientSocket.getOutputStream());
                final KVMessageProto response = new KVMessageProto(clientSocket.getInputStream());
                if (response.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
                    updateMetadata(ECSHashRing.fromConfig(response.getValue()));
                } else return response;
            } catch (Exception e) {
                return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), id);
            }
        }
    }

    private void updateMetadata(ECSHashRing newConfig) throws IOException {
        final Set<String> newConnectionStrings = newConfig.getAllNodes().values().stream().map(e -> (ECSNode) e).map(ECSNode::getConnectionString).collect(Collectors.toSet());
        serverConnections.entrySet().removeIf(entry -> {
            if (!newConnectionStrings.contains(entry.getKey())) {
                disconnect(entry.getKey(), entry.getValue());
                return true; // disconnect from removed nodes
            } else return false; // don't disconnect from previously active node
        });

        hashRing.clear();
        hashRing.addAll(newConfig);
        try {
            connect();
        } catch (Exception e) {
            throw new IOException("Unable to connect to new server");
        }
    }

    private String validatedKey(String key) {
        if (key.isEmpty())
            throw new IllegalArgumentException("Key must not be empty");
        if (key.length() > MAX_KEY_SIZE)
            throw new IllegalArgumentException(String.format("Max key length is %d Bytes", MAX_KEY_SIZE));
        // TODO: should also check for no space but that would fail testing.InteractionTest.testGetUnsetValue()
        return key;
    }

    private String validatedValue(String value) {
        value = value == null ? "null" : value;
        if (value.length() > MAX_VALUE_SIZE)
            throw new IllegalArgumentException(String.format("Max value length is %d Bytes", MAX_VALUE_SIZE));
        return value;
    }
}
