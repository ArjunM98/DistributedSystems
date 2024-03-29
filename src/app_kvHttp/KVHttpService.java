package app_kvHttp;

import app_kvECS.ECSClient;
import app_kvHttp.controller.Handler;
import app_kvHttp.controller.KvHandler;
import app_kvHttp.controller.QueryHandler;
import client.KVStorePool;
import com.sun.net.httpserver.HttpServer;
import ecs.zk.ZooKeeperService;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVHttpService {
    private static final Logger logger = Logger.getRootLogger();

    private static final int NUM_WORKERS = 16;

    private final HttpServer httpServer;
    private final ExecutorService httpWorkers;
    private final ZooKeeperService zk;
    private final KVStorePool kvStorePool;

    /**
     * Create an HTTP service for the M4 extension
     *
     * @param port             to listen for HTTP connections on
     * @param connectionString to watch for ECS changes on
     */
    public KVHttpService(int port, String connectionString) {
        // 1. Initialize ZooKeeper connection
        try {
            // First establish connections
            this.zk = new ZooKeeperService(connectionString);
            this.kvStorePool = new KVStorePool(NUM_WORKERS);

            // Then preemptively fetch metadata information
            this.zk.initializeRootNodesForEcs(ECSClient.ECS_NAME);
            this.kvStorePool.updateMetadata(this.zk.getData(ZooKeeperService.ZK_METADATA));
            this.zk.watchDataForever(ZooKeeperService.ZK_METADATA, kvStorePool::updateMetadata);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect to KV Service", e);
        }

        // 2. Spin up HTTP server
        try {
            this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            this.httpServer.setExecutor(this.httpWorkers = Executors.newFixedThreadPool(NUM_WORKERS));
            this.httpServer.createContext("/", new Handler.NotFoundHandler());
            this.httpServer.createContext(KvHandler.PATH_PREFIX, new KvHandler(this.kvStorePool));
            this.httpServer.createContext(QueryHandler.PATH_PREFIX, new QueryHandler(this.kvStorePool));
        } catch (IOException e) {
            throw new RuntimeException("Unable to create HTTP server", e);
        }
        this.httpServer.start();
        logger.info("HTTP service started");
        logger.info("portNumber = " + port);
        logger.info("connectionString = " + connectionString);
    }

    public void close() {
        logger.info("Terminating KVHttpService...");

        try {
            this.httpWorkers.shutdownNow();
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate threads", e);
        }

        try {
            this.httpServer.stop(5);
        } catch (Exception e) {
            logger.error("Unable to terminate server", e);
        }

        try {
            zk.close();
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate ZooKeeper connection", e);
        }

        try {
            kvStorePool.close();
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate KV service connections", e);
        }

    }

    /**
     * Main entry point for the KVHttpService application.
     *
     * @param args contains [portNumber, zkConn [, logLevel]]
     */
    public static void main(String[] args) {
        // 0. Default args
        int portNumber;
        String connectionString;
        Level logLevel = Level.ALL;

        // 1. Validate args
        try {
            switch (args.length) {
                case 3:
                    String candidateLevel = args[2].toUpperCase();
                    if (!LogSetup.isValidLevel(candidateLevel))
                        throw new IllegalArgumentException(String.format("Invalid log level '%s'", candidateLevel));
                    logLevel = Level.toLevel(candidateLevel, logLevel);
                case 2:
                    connectionString = args[1];
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
            System.err.println("Usage: Http <port> <connectionString> [<loglevel>]");
            System.exit(1);
            return;
        }

        // 2. Initialize logger
        try {
            new LogSetup("logs/http.log", logLevel);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 3. Run server and respond to ctrl-c and kill
        final KVHttpService kvHttpService = new KVHttpService(portNumber, connectionString);
        Runtime.getRuntime().addShutdownHook(new Thread(kvHttpService::close));
    }
}
