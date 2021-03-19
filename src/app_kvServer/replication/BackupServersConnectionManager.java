package app_kvServer.replication;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static app_kvServer.storage.IKVStorage.KVPair;

/**
 * Used by a primary server to write to its backups
 */
public class BackupServersConnectionManager {
    private static final Logger logger = Logger.getRootLogger();

    private final ExecutorService threadPool;
    private final Map<String, BackupServerConnection> backupServers;

    public BackupServersConnectionManager() {
        this.threadPool = Executors.newFixedThreadPool(2);
        this.backupServers = new HashMap<>();
    }

    public synchronized void connect(String server, String hostname, int port) throws IOException {
        logger.info("Connecting to backup: " + server);
        backupServers.put(server, new BackupServerConnection(server, hostname, port, backupServers::remove));
    }

    public synchronized void disconnect(String server) {
        logger.info("Disconnecting from backup: " + server);
        final BackupServerConnection connection = backupServers.remove(server);
        if (connection != null) connection.close();
    }

    public void replicate(KVPair kv) {
        logger.info(String.format("Replicating to %d servers", backupServers.size()));
        backupServers.values().forEach(replica -> threadPool.execute(() -> replica.replicate(kv)));
    }

    public synchronized void close() {
        try {
            threadPool.shutdownNow();
            logger.warn(String.format("%d replicas were active", backupServers.size()));
            new ArrayList<>(backupServers.keySet()).forEach(this::disconnect);
        } catch (Exception e) {
            logger.error("Unable to cleanly terminate threads", e);
        }
    }

    /**
     * Helper class that lets us send data to another server
     */
    public static class BackupServerConnection {
        private final String serverName;
        private final PrintWriter replicaWriter;
        private final Consumer<String> onDisconnect;

        public BackupServerConnection(String serverName, String hostname, int port, Consumer<String> onDisconnect) throws IOException {
            this.serverName = serverName;
            this.replicaWriter = new PrintWriter(new OutputStreamWriter(new Socket(hostname, port).getOutputStream()), true);
            this.onDisconnect = onDisconnect;
        }

        public void replicate(KVPair kv) {
            try {
                logger.info("Replicating " + kv.tombstone + " " + kv.key + " to " + serverName);
                this.replicaWriter.println(kv.serialize());
            } catch (Exception e) {
                logger.warn("Replication op " + kv.tombstone + " " + kv.key + " to " + serverName + " failed: " + e.getMessage());
                close();
            }
        }

        public void close() {
            this.replicaWriter.close();
            this.onDisconnect.accept(this.serverName);
        }
    }
}
