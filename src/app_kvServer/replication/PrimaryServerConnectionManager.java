package app_kvServer.replication;

import app_kvServer.KVServer;
import app_kvServer.storage.IKVStorage.KVPair;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Used by a replica server to read from its primaries
 */
public class PrimaryServerConnectionManager extends Thread {
    private static final Logger logger = Logger.getRootLogger();
    private static final int NUM_PRIMARIES = 2;

    private final ServerSocket p2pServerSocket;
    private final KVServer replicaServer;

    private final ExecutorService threadPool;
    private final Set<PrimaryServerConnection> activePrimaries;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public PrimaryServerConnectionManager(KVServer replicaServer) throws IOException {
        this.replicaServer = replicaServer;
        this.p2pServerSocket = new ServerSocket(0);
        this.threadPool = Executors.newFixedThreadPool(NUM_PRIMARIES);
        this.activePrimaries = new HashSet<>();
        start();
    }

    public int getPort() {
        return this.p2pServerSocket.getLocalPort();
    }

    @Override
    public void run() {
        this.isRunning.set(true);
        logger.info("Listening for primaries on port " + getPort());
        while (this.isRunning.get()) {
            try {
                Socket replica = p2pServerSocket.accept();
                logger.debug("New primary:" + replica);
                final PrimaryServerConnection connection = new PrimaryServerConnection(
                        replica,
                        replicaServer,
                        activePrimaries::remove
                );
                threadPool.execute(connection);
                activePrimaries.add(connection);
            } catch (IOException e) {
                logger.warn("Socket error: " + e.getMessage());
            } catch (Exception e) {
                logger.error("Primary failed", e);
            }
        }
        logger.info("BackupServerConnectionManager exiting...");
    }

    public void close() {
        if (this.isRunning.get()) {
            this.isRunning.set(false);
            try {
                threadPool.shutdownNow();
                logger.warn(String.format("%d replicas were active", activePrimaries.size()));
                activePrimaries.forEach(PrimaryServerConnection::close);
            } catch (Exception e) {
                logger.error("Unable to cleanly terminate threads", e);
            }

            try {
                p2pServerSocket.close();
            } catch (Exception e) {
                logger.warn("Unable to cleanly terminate socket: " + e.getMessage());
            }
        } else {
            logger.warn("BackupServerConnectionManager already closed");
        }
    }

    /**
     * Runnable that lets us accept data from another server
     */
    public static class PrimaryServerConnection implements Runnable {
        private final Socket primarySocket;
        private final KVServer replicaServer;
        private final Consumer<PrimaryServerConnection> onDisconnect;

        public PrimaryServerConnection(Socket primarySocket, KVServer replicaServer, Consumer<PrimaryServerConnection> onDisconnect) {
            this.primarySocket = primarySocket;
            this.replicaServer = replicaServer;
            this.onDisconnect = onDisconnect;
        }

        @Override
        public void run() {
            logger.info("PRIMARY LISTENER STARTED");
            try (Stream<String> s = new BufferedReader(new InputStreamReader(primarySocket.getInputStream())).lines()) {
                s.forEach(message -> {
                    final KVPair kv = KVPair.deserialize(message);
                    logger.info("Received replication event: " + kv.tombstone + " " + kv.key);
                    replicaServer.forceIngestKV(kv);
                });
            } catch (IOException | UncheckedIOException e) {
                logger.info("Socket error: " + e.getMessage());
            } catch (Exception e) {
                logger.error("Error occurred on primary connection", e);
            }
            this.onDisconnect.accept(this);
        }

        public void close() {
            try {
                primarySocket.close();
            } catch (IOException e) {
                logger.warn("Unable to close primary socket: " + e.getMessage());
            }
        }
    }
}
