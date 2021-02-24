package app_kvServer;

import app_kvServer.cache.IKVCache;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.IKVStorage.KVPair;
import app_kvServer.storage.KVPartitionedStorage;
import ecs.ECSHashRing;
import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class KVServer extends Thread implements IKVServer {
    private static final Logger logger = Logger.getRootLogger();

    private final int port;
    private final IKVCache cache;
    private final IKVStorage storage;

    private final ExecutorService threadPool;
    private ServerSocket serverSocket;

    /**
     * TODO (@ravi): encapsulate and provide this functionality from some kind of server state class that deals with all the ECS things
     * Also will need to break this into at least the 3 following cases:
     * - isAlive - server is alive
     * - isStarted - server is alive and allowed to respond to clients
     * - isWriteLocked - see {@link shared.messages.KVMessage.StatusType#SERVER_WRITE_LOCK}
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * TODO (@ravi): encapsulate these two and properly init/update/etc.
     */
    private final ECSNode myEcsNode;
    private final ECSHashRing<ECSNode> allEcsNodes;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.threadPool = Executors.newCachedThreadPool();

        this.storage = new KVPartitionedStorage(IKVStorage.STORAGE_ROOT_DIRECTORY + "/" + port);
        CacheStrategy cacheStrategy = CacheStrategy.None;
        try {
            cacheStrategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.warn("Defaulting to no cache", e);
        } finally {
            this.cache = IKVCache.newInstance(cacheStrategy, cacheSize);
        }

        // TODO: init and reinit these two properly somewhere else
        allEcsNodes = ECSHashRing.fromConfig(String.format("server1 localhost %d", port), ECSNode::fromConfig);
        myEcsNode = allEcsNodes.getServer(String.format("localhost:%d", port));

        this.start();
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getHostname() {
        if (serverSocket == null) try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("Unknown host: try starting the server first", e);
        }
        return serverSocket.getInetAddress().getHostName();
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
        if (key.equals("todo: do a stopped state check here")) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (!myEcsNode.isResponsibleForKey(key)) {
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
        if (key.equals("todo: do a stopped state check here")) {
            throw new KVServerException("Server is in STOPPED state", KVMessage.StatusType.SERVER_STOPPED);
        }

        if (key.equals("todo: do a write lock check here")) {
            throw new KVServerException("Server is locked for writes", KVMessage.StatusType.SERVER_WRITE_LOCK);
        }

        if (!myEcsNode.isResponsibleForKey(key)) {
            throw new KVServerException(String.format("Server not responsible for key '%s'", key), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        // TODO: consider locking cache and storage together https://stackoverflow.com/q/5639870
        if ("null".equals(value)) try {
            // Delete from cache before deleting from storage so other clients don't use the old cached value
            // and instead have to read from storage which is protected by a lock
            cache.delete(key);
            storage.delete(key);
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException(String.format("Unknown error processing key '%s'", key), e, KVMessage.StatusType.FAILED);
        }
        else try {
            // Store BEFORE caching in case of any failures
            storage.putKV(key, value);
            cache.putKV(key, value);
        } catch (KVServerException e) {
            throw e;
        } catch (Exception e) {
            throw new KVServerException(String.format("Unknown error processing key '%s'", key), e, KVMessage.StatusType.FAILED);
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
                threadPool.execute(new ClientConnection(client, this /* reference to server process */));
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
        this.isRunning.set(false);
        try {
            List<Runnable> awaitingClients = threadPool.shutdownNow();
            logger.warn(String.format("%d clients were active", awaitingClients.size()));
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate threads", e);
        }

        try {
            serverSocket.close();
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate socket", e);
        }

        this.clearCache();
    }

    @Override
    public void close() {
        final long TIMEOUT_MILLIS = 5000L; // how long to wait for threads to cleanup

        this.isRunning.set(false);
        try {
            threadPool.shutdown();
            if (!threadPool.awaitTermination(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                logger.warn("Some clients may still be active (termination wait timeout)");
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate", e);
        }

        try {
            serverSocket.close();
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate socket", e);
        }

        this.clearCache();
    }

    /**
     * TODO: idk if this is the implementation we'll be using once ECS module is in place
     */
    public String getMetadata() {
        return this.allEcsNodes.toConfig();
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
     * Main entry point for the KVServer application.
     *
     * @param args contains [portNumber [, cacheSize, policy, logLevel]]
     */
    public static void main(String[] args) {
        int portNumber, cacheSize = 10;
        String policy = "FIFO";
        Level logLevel = Level.ALL;

        // 1. Validate args
        try {
            switch (args.length) {
                case 4:
                    String candidateLevel = args[3].toUpperCase();
                    if (!LogSetup.isValidLevel(candidateLevel))
                        throw new IllegalArgumentException(String.format("Invalid log level '%s'", candidateLevel));
                    logLevel = Level.toLevel(candidateLevel, Level.ALL);
                case 3:
                    String candidatePolicy = args[2].toUpperCase();
                    if (Arrays.stream(CacheStrategy.values()).noneMatch(e -> e.name().equals(candidatePolicy)))
                        throw new IllegalArgumentException(String.format("Invalid cache policy '%s'", candidatePolicy));
                    policy = candidatePolicy;
                case 2:
                    try {
                        cacheSize = Integer.parseInt(args[1]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("Invalid cache size '%s'", args[1]));
                    }
                case 1:
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
            System.err.println("Usage: Server <port> [<cachesize> <cachepolicy> <loglevel>]");
            System.exit(1);
            return;
        }

        // 2. Initialize logger
        try {
            new LogSetup("logs/server.log", logLevel);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 3. Run server and respond to ctrl-c and kill
        final KVServer kvServer = (KVServer) ObjectFactory.createKVServerObject(portNumber, cacheSize, policy);
        Runtime.getRuntime().addShutdownHook(new Thread(kvServer::close));
    }
}
