package app_kvServer;

import app_kvHttp.model.Model;
import app_kvHttp.model.request.Query;
import app_kvHttp.model.request.Remapping;
import app_kvServer.cache.IKVCache;
import app_kvServer.replication.BackupServersConnectionManager;
import app_kvServer.replication.PrimaryServerConnectionManager;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.IKVStorage.KVPair;
import app_kvServer.storage.KVPartitionedStorage;
import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;
import shared.Utilities;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KVServer extends Thread implements IKVServer {
    private static final Logger logger = Logger.getRootLogger();

    private final String name;
    private final int port;
    private final IKVCache cache;
    private final IKVStorage storage;

    private final ExecutorService threadPool;
    private final Set<ClientConnection> activeConnections;
    private ServerSocket serverSocket;
    private ECSServerConnection.State state;
    private final ECSServerConnection ecsServerConnection;

    private BackupServersConnectionManager backupServersConnectionManager;
    private PrimaryServerConnectionManager primaryServerConnectionManager;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * Start KV Server at given port
     *
     * @param port             given port for storage server to operate
     * @param name             server name
     * @param connectionString connection string used for ZooKeeper
     * @param cacheSize        specifies how many key-value pairs the server is allowed
     *                         to keep in-memory
     * @param strategy         specifies the cache replacement strategy in case the cache
     *                         is full and there is a GET- or PUT-request on a key that is
     *                         currently not contained in the cache. Options are "FIFO", "LRU",
     *                         and "LFU".
     */
    public KVServer(int port, String name, String connectionString, int cacheSize, String strategy) {
        this.name = name;
        this.port = port;
        this.state = ECSServerConnection.State.STOPPED;

        try {
            this.ecsServerConnection = new ECSServerConnection(this, connectionString);
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to ZooKeeper", e);
        }

        this.threadPool = Executors.newCachedThreadPool();
        this.activeConnections = new HashSet<>();

        this.storage = new KVPartitionedStorage(IKVStorage.STORAGE_ROOT_DIRECTORY + "/" + port);
        CacheStrategy cacheStrategy = CacheStrategy.None;
        try {
            cacheStrategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.warn("Defaulting to no cache", e);
        } finally {
            this.cache = IKVCache.newInstance(cacheStrategy, cacheSize);
        }

        this.start();
    }

    public void updateServerState(ECSServerConnection.State newState) {
        this.state = newState;
    }

    public String getServerName() {
        return name;
    }

    @Override
    public int getPort() {
        return port;
    }

    public String getMetadata() {
        return this.ecsServerConnection.getMetadata();
    }

    @Override
    public String getHostname() {
        return Utilities.getHostname();
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return cache.getCacheStrategy();
    }

    @Override
    public int getCacheSize() {
        return cache.getCacheSize();
    }

    @Override
    public boolean inStorage(String key) {
        return storage.inStorage(key);
    }

    @Override
    public boolean inCache(String key) {
        return cache.inCache(key);
    }

    @Override
    public String getKV(String key) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (!this.ecsServerConnection.isResponsibleForKey(key, true)) {
            throw new KVServerException(String.format("Server not responsible for key '%s'", key), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        try {
            String value;

            if ((value = cache.getKV(key)) != null) {
                logger.debug(String.format("Key '%s' found in cache", key));
                return value;
            }

            if ((value = storage.getKV(key)) != null) {
                cache.putKV(key, value);
                logger.debug(String.format("Key '%s' found in storage", key));
                return value;
            }

            throw new KVServerException(String.format("No mapping for key '%s'", key), KVMessage.StatusType.GET_ERROR);
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException(String.format("Unknown error processing key '%s'", key), e, KVMessage.StatusType.FAILED);
        }
    }

    @Override
    public void putKV(String key, String value) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (state == ECSServerConnection.State.LOCKED) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        if (!ecsServerConnection.isResponsibleForKey(key, false)) {
            throw new KVServerException(String.format("Server not responsible for key '%s'", key), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        // TODO: consider locking cache and storage together https://stackoverflow.com/q/5639870
        if ("null".equals(value)) try {
            // Delete from cache before deleting from storage so other clients don't use the old cached value
            // and instead have to read from storage which is protected by a lock
            cache.delete(key);
            storage.delete(key);

            backupServersConnectionManager.replicate(new KVPair(KVPair.Tombstone.DEAD, key, ""));
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException(String.format("Unknown error processing key '%s'", key), e, KVMessage.StatusType.FAILED);
        }
        else try {
            // Store BEFORE caching in case of any failures
            storage.putKV(key, value);
            cache.putKV(key, value);

            backupServersConnectionManager.replicate(new KVPair(KVPair.Tombstone.VALID, key, value));
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException(String.format("Unknown error processing key '%s'", key), e, KVMessage.StatusType.FAILED);
        }
    }

    public String coordinateGetAllKV(Query filter) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        String lockPath;
        try {
            lockPath = ecsServerConnection.lock(true);
        } catch (IOException e) {
            throw new KVServerException("Unable to acquire read lock", e, KVMessage.StatusType.FAILED);
        }

        try {
            List<Callable<KVMessage>> tasks = new ArrayList<>();

            // 1. Establish a temporary connection to each server in hash ring as a client and send request
            for (ECSNode node : ecsServerConnection.getAllServers()) {
                tasks.add(() -> {
                    // 1a. Connect to specified server
                    Socket socket = new Socket(node.getNodeHost(), node.getNodePort());

                    // 1b. Send message to get all
                    new KVMessageProto(KVMessage.StatusType.GET_ALL, Model.toString(filter), 0 /* only 1 request sent over this connection */)
                            .writeMessageTo(socket.getOutputStream());

                    // 1c. Wait for response
                    KVMessageProto response = new KVMessageProto(socket.getInputStream());

                    // 1d. Disconnect from server
                    socket.close();

                    return response;
                });
            }

            List<KVMessage> allPairs = new ArrayList<>();

            // 2. Get result
            try {
                for (Future<KVMessage> result : threadPool.invokeAll(tasks, 5, TimeUnit.MINUTES)) {
                    KVMessage res = result.get(5, TimeUnit.MINUTES);
                    logger.debug(String.format("%s", res.getStatus()));
                    allPairs.add(res);
                }
            } catch (Exception e) {
                throw new KVServerException("Unable to gather all relevant keys", e, KVMessage.StatusType.FAILED);
            }

            // 3. Check for at least one success
            allPairs = allPairs.stream()
                    .filter(msg -> msg.getStatus() == KVMessage.StatusType.GET_ALL_SUCCESS)
                    .collect(Collectors.toList());

            // 4.a if success, consolidate results
            if (allPairs.size() > 0)
                return allPairs.stream().map(KVMessage::getValue).collect(Collectors.joining("\n"));

            // 4.b if not, throw and propagate
            throw new KVServerException("No keys matching filter", KVMessage.StatusType.COORDINATE_GET_ALL_ERROR);
        } finally {
            ecsServerConnection.unlock(lockPath);
        }
    }

    public String getAllKV(Query filter) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        final Predicate<String> keyPredicate = filter.getKeyFilter().asMatchPredicate()
                .and(key -> ecsServerConnection.isResponsibleForKey(key, false));
        final Predicate<String> valuePredicate = filter.getValueFilter().asMatchPredicate();

        try {
            List<KVPair> value = storage.getAllKV(kv -> keyPredicate.test(kv.key) && valuePredicate.test(kv.value));

            if (!value.isEmpty()) return value.stream().map(KVPair::serialize).collect(Collectors.joining("\n"));

            logger.debug("Unable to find any keys with expression");
            throw new KVServerException("No keys matching filter", KVMessage.StatusType.GET_ALL_ERROR);
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException("Unknown error processing filter expression", e, KVMessage.StatusType.FAILED);
        }
    }

    public String coordinatePutAllKV(Query filter, Remapping mapping) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (state == ECSServerConnection.State.LOCKED) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        String lockPath;
        try {
            lockPath = ecsServerConnection.lock(false);
        } catch (IOException e) {
            throw new KVServerException("Unable to acquire write lock", e, KVMessage.StatusType.FAILED);
        }

        try {
            List<Callable<KVMessage>> tasks = new ArrayList<>();

            // 1. Establish a temporary connection to each server in hash ring as a client and send request
            for (ECSNode node : ecsServerConnection.getAllServers()) {
                tasks.add(() -> {
                    // 1a. Connect to specified server
                    Socket socket = new Socket(node.getNodeHost(), node.getNodePort());

                    // 1b. Send message to get all
                    new KVMessageProto(KVMessage.StatusType.PUT_ALL, Model.toString(filter), Model.toString(mapping), 0 /* only 1 request sent over this connection */)
                            .writeMessageTo(socket.getOutputStream());

                    // 1c. Wait for response
                    KVMessageProto response = new KVMessageProto(socket.getInputStream());

                    // 1d. Disconnect from server
                    socket.close();

                    return response;
                });
            }

            List<KVMessage> allUpdatedVals = new ArrayList<>();

            // 2. Put result
            try {
                for (Future<KVMessage> result : threadPool.invokeAll(tasks, 5, TimeUnit.MINUTES)) {
                    KVMessage res = result.get(5, TimeUnit.MINUTES);
                    logger.debug(String.format("%s", res.getStatus()));
                    allUpdatedVals.add(res);
                }
            } catch (Exception e) {
                throw new KVServerException("Unable to update all relevant keys", e, KVMessage.StatusType.FAILED);
            }

            // 3. Filter down relevant results
            allUpdatedVals = allUpdatedVals.stream()
                    .filter(msg -> msg.getStatus() == KVMessage.StatusType.PUT_ALL_SUCCESS)
                    .collect(Collectors.toList());

            if (allUpdatedVals.size() > 0)
                return allUpdatedVals.stream().map(KVMessage::getValue).collect(Collectors.joining("\n"));

            throw new KVServerException("No keys matching filter", KVMessage.StatusType.COORDINATE_PUT_ALL_ERROR);
        } finally {
            ecsServerConnection.unlock(lockPath);
        }
    }

    public String putAllKV(Query filter, Remapping mapping) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (state == ECSServerConnection.State.LOCKED) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        // Clear cache to get rid of stale values
        cache.clearCache();

        final Predicate<String> keyPredicate = filter.getKeyFilter().asMatchPredicate();
        final Predicate<String> valuePredicate = filter.getValueFilter().asMatchPredicate();

        try {
            List<KVPair> value = storage.putAllKV(kv -> keyPredicate.test(kv.key) && valuePredicate.test(kv.value),
                    mapping.getFind().pattern(), mapping.getReplace());

            if (!value.isEmpty()) {
                // Only send back keys where you are the primary
                return value.stream()
                        .filter(kv -> ecsServerConnection.isResponsibleForKey(kv.key, false))
                        .map(IKVStorage.KVPair::serialize)
                        .collect(Collectors.joining("\n"));
            }

            logger.debug("Unable to find any keys with expression");
            throw new KVServerException("No keys matching filter", KVMessage.StatusType.PUT_ALL_ERROR);
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException("Unknown error processing filter expression", e, KVMessage.StatusType.FAILED);
        }
    }

    public void coordinateDeleteAllKV(Query filter) throws KVServerException {

        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (state == ECSServerConnection.State.LOCKED) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        String lockPath;
        try {
            lockPath = ecsServerConnection.lock(false);
        } catch (IOException e) {
            throw new KVServerException("Unable to acquire write lock", e, KVMessage.StatusType.FAILED);
        }

        try {
            List<Callable<KVMessage>> tasks = new ArrayList<>();

            // 1. Establish a temporary connection to each server in hash ring as a client and send request
            for (ECSNode node : ecsServerConnection.getAllServers()) {
                tasks.add(() -> {
                    // 1a. Connect to specified server
                    Socket socket = new Socket(node.getNodeHost(), node.getNodePort());

                    // 1b. Send message to get all
                    new KVMessageProto(KVMessage.StatusType.DELETE_ALL, Model.toString(filter), 0 /* only 1 request sent over this connection */)
                            .writeMessageTo(socket.getOutputStream());

                    // 1c. Wait for response
                    KVMessageProto response = new KVMessageProto(socket.getInputStream());

                    // 1d. Disconnect from server
                    socket.close();

                    return response;
                });
            }

            List<KVMessage> deletedVals = new ArrayList<>();

            // 2. delete result
            try {
                for (Future<KVMessage> result : threadPool.invokeAll(tasks, 5, TimeUnit.MINUTES)) {
                    KVMessage res = result.get(5, TimeUnit.MINUTES);
                    logger.debug(String.format("%s", res.getStatus()));
                    deletedVals.add(res);
                }
            } catch (Exception e) {
                throw new KVServerException("Unable to update all relevant keys", e, KVMessage.StatusType.FAILED);
            }

            // 3. Filter down relevant results
            deletedVals = deletedVals.stream()
                    .filter(msg -> msg.getStatus() == KVMessage.StatusType.DELETE_ALL_SUCCESS)
                    .collect(Collectors.toList());

            if (deletedVals.size() <= 0) {
                throw new KVServerException("No keys matching filter", KVMessage.StatusType.COORDINATE_DELETE_ALL_ERROR);
            }
        } finally {
            ecsServerConnection.unlock(lockPath);
        }
    }

    public void deleteAll(Query filter) throws KVServerException {
        if (state == ECSServerConnection.State.STOPPED) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (state == ECSServerConnection.State.LOCKED) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        cache.clearCache(); // expensive af but much simpler than actually pruning cache

        final Predicate<String> keyPredicate = filter.getKeyFilter().asMatchPredicate();
        final Predicate<String> valuePredicate = filter.getValueFilter().asMatchPredicate();

        try {
            storage.deleteIf(kv -> keyPredicate.test(kv.key) && valuePredicate.test(kv.value));
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException("Unknown error processing filter expression", e, KVMessage.StatusType.FAILED);
        }
    }

    @Override
    public void clearCache() {
        cache.clearCache();
        logger.info("Cleared cache");
    }

    @Override
    public void clearStorage() {
        storage.clearStorage();
        logger.info("Cleared storage");
    }

    @Override
    public void run() {
        logger.info("Initializing server...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Bound to port " + port);

            primaryServerConnectionManager = new PrimaryServerConnectionManager(this);
            backupServersConnectionManager = new BackupServersConnectionManager();

            this.isRunning.set(true);
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
        }

        logger.debug("kvServer.getCacheSize() = " + this.getCacheSize());
        logger.debug("kvServer.getCacheStrategy() = " + this.getCacheStrategy());
        logger.debug("kvServer.getHostname() = " + this.getHostname());
        logger.debug("kvServer.getPort() = " + this.getPort());

        while (this.isRunning.get()) {
            try {
                // TODO: look into socket config e.g. timeout, keepalive, tcp optimization, ...
                Socket client = serverSocket.accept();
                logger.debug("New client:" + client);
                final ClientConnection connection = new ClientConnection(
                        client,
                        this /* reference to server process */,
                        activeConnections::remove
                );
                threadPool.execute(connection);
                activeConnections.add(connection);
            } catch (IOException e) {
                logger.warn("Socket error: " + e.getMessage());
            } catch (RejectedExecutionException e) {
                logger.error("Client rejected", e);
            }
        }
        logger.info("Server exiting...");
    }

    @Override
    public void kill() {
        if (this.isRunning.get()) {
            this.isRunning.set(false);
            try {
                threadPool.shutdownNow();
                logger.warn(String.format("%d clients were active", activeConnections.size()));
                activeConnections.forEach(ClientConnection::close);
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate threads", e);
            }

            try {
                serverSocket.close();
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate socket", e);
            }

            try {
                ecsServerConnection.close();
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate ECS connection", e);
            }

            try {
                primaryServerConnectionManager.close();
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate replica (get) connection", e);
            }

            try {
                backupServersConnectionManager.close();
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate replica (send) connection", e);
            }
        } else {
            logger.info(String.format("Second call: %d", java.lang.Thread.activeCount()));
            logger.warn("Server already closed");
        }
    }

    @Override
    public void close() {
        this.kill();
    }

    /**
     * Stream all KVs from (a temp snapshot of) storage. Remember to call {@link Stream#close()} on the resulting
     * stream after it's been processed (see {@link IKVStorage#openKvStream(Predicate)} for explanation)
     *
     * @return a stream of serialized {@link KVPair}s
     */
    public Stream<String> openKvStream(Predicate<KVPair> filter) {
        return storage.openKvStream(filter).map(KVPair::serialize);
    }

    /**
     * Bulk PUT operation given a stream of serialized {@link KVPair}s
     *
     * @param serializedKvStream like the results from {@link #openKvStream(Predicate)} but could be any serialized kv
     *                           string stream e.g. one coming out of a socket
     */
    public void putAllFromKvStream(Stream<String> serializedKvStream) {
        this.clearCache();
        try (serializedKvStream) {
            serializedKvStream.map(KVPair::deserialize).filter(Objects::nonNull).forEach(kv -> {
                try {
                    storage.putKV(kv.key, kv.value);
                } catch (KVServerException e) {
                    logger.info(String.format("Error ingesting kv '%s'", kv.key));
                }
            });
        }
    }

    /**
     * See {@link IKVStorage#deleteIf(Predicate)}
     */
    public void deleteIf(Predicate<KVPair> filter) {
        try {
            storage.deleteIf(filter);
        } catch (KVServerException e) {
            logger.error("Unable to clear designated KV", e);
        }
        cache.clearCache(); // expensive af but much simpler than actually pruning cache
    }

    /**
     * @return replica outbound connections manager
     */
    public BackupServersConnectionManager getBackupServerManager() {
        return this.backupServersConnectionManager;
    }

    /**
     * @return replica inbound connections manager
     */
    public PrimaryServerConnectionManager getPrimaryServerConnectionManager() {
        return this.primaryServerConnectionManager;
    }

    /**
     * Perform a KV ingestion operation without checking for hash range, lock, etc.
     *
     * @param kv from a coordinator server that this is a replica of
     */
    public void forceIngestKV(KVPair kv) {
        try {
            switch (kv.tombstone) {
                case VALID:
                    storage.putKV(kv.key, kv.value);
                    break;
                case DEAD:
                    storage.delete(kv.key);
                    break;
            }
        } catch (KVServerException e) {
            logger.info(String.format("Error ingesting kv '%s'", kv.key));
        }
    }

    /**
     * Main entry point for the KVServer application.
     *
     * @param args contains [portNumber, name, zkConn [, cacheSize, policy, logLevel]]
     */
    public static void main(String[] args) {
        // 0. Default args
        int portNumber, cacheSize = 10;
        String name;
        String connectionString;
        String policy = "FIFO";
        Level logLevel = Level.ALL;

        // 1. Validate args
        try {
            switch (args.length) {
                case 6:
                    String candidateLevel = args[5].toUpperCase();
                    if (!LogSetup.isValidLevel(candidateLevel))
                        throw new IllegalArgumentException(String.format("Invalid log level '%s'", candidateLevel));
                    logLevel = Level.toLevel(candidateLevel, logLevel);
                case 5:
                    String candidatePolicy = args[4].toUpperCase();
                    if (Arrays.stream(CacheStrategy.values()).noneMatch(e -> e.name().equals(candidatePolicy)))
                        throw new IllegalArgumentException(String.format("Invalid cache policy '%s'", candidatePolicy));
                    policy = candidatePolicy;
                case 4:
                    try {
                        cacheSize = Integer.parseInt(args[3]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("Invalid cache size '%s'", args[1]));
                    }
                case 3:
                    name = args[1];
                    connectionString = args[2];
                    try {
                        portNumber = Integer.parseInt(args[0]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("Invalid port number '%s'", args[0]));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Invalid number of arguments");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e);
            System.err.println("Usage: Server <port> <name> <connectionString> [<cachesize> <cachepolicy> <loglevel>]");
            System.exit(1);
            return;
        }

        // 2. Initialize logger
        try {
            new LogSetup("logs/" + name + "_" + System.currentTimeMillis() + ".log", logLevel);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 3. Run server and respond to ctrl-c and kill
        final KVServer kvServer = (KVServer) ObjectFactory.createKVServerObject(portNumber, name, connectionString, cacheSize, policy);
        Runtime.getRuntime().addShutdownHook(new Thread(kvServer::close));
    }
}
