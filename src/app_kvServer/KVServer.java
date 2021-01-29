package app_kvServer;

import app_kvServer.cache.IKVCache;
import app_kvServer.storage.IKVStorage;
import app_kvServer.storage.KVPartitionedStorage;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KVServer extends Thread implements IKVServer {
    private static final Logger logger = Logger.getRootLogger();

    private final int port;
    private final IKVCache cache;
    private final IKVStorage storage;

    private final ExecutorService threadPool;
    private ServerSocket serverSocket;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

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

        CacheStrategy cacheStrategy = CacheStrategy.None;
        try {
            cacheStrategy = CacheStrategy.valueOf(strategy);
        } catch (IllegalArgumentException e) {
            logger.warn("Defaulting to no cache", e);
        } finally {
            this.cache = IKVCache.newInstance(cacheStrategy, cacheSize);
        }

        storage = new KVPartitionedStorage();
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
    public String getKV(String key) throws Exception {
        if (inCache(key)) {
            logger.debug(String.format("Key '%s' found in cache", key));
            return cache.getKV(key);
        }

        if (inStorage(key)) {
            logger.debug(String.format("Key '%s' found in storage", key));
            return storage.getKV(key);
        }

        throw new IllegalArgumentException(String.format("No mapping for key '%s'", key));
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // TODO: concurrency bugs in both cases where cache may be wrongly updated if storage fails
        // look into https://stackoverflow.com/q/5639870
        if ("null".equals(value)) {
            cache.delete(key);
            storage.delete(key);
        } else {
            cache.putKV(key, value);
            storage.putKV(key, value);
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
                Socket client = serverSocket.accept();
                logger.debug("New client:" + client);
                threadPool.execute(new ClientConnection(client, this /* reference to server process */));
            } catch (IOException e) {
                logger.error("Socket error", e);
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
        } catch (IOException e) {
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
        } catch (IOException e) {
            logger.error("Unable to cleanly terminate socket", e);
        }

        this.clearCache();
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
        kvServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kvServer::close));
    }
}
