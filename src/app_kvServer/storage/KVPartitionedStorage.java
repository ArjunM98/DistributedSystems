package app_kvServer.storage;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class KVPartitionedStorage implements IKVStorage {
    private static final Logger logger = Logger.getRootLogger();
    private static final String KV_DELIMITER = " ";

    private static final int NUM_PERSISTENT_STORES = 8;
    private static final ILoadBalancer<ConcurrentFileStore> loadBalancer = ConcurrentFileStore.createLoadBalancer(NUM_PERSISTENT_STORES);

    private final List<ConcurrentFileStore> stores;

    public KVPartitionedStorage() {
        stores = IntStream.rangeClosed(1, NUM_PERSISTENT_STORES)
                .mapToObj(i -> new ConcurrentFileStore("data", String.format("store%d.txt", i)))
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public boolean inStorage(String key) {
        return getKV(key) != null;
    }

    @Override
    public String getKV(String key) {
        final ConcurrentFileStore store = loadBalancer.balanceRequest(key, stores);
        try {
            store.lock.readLock().lock();
            return store.readFromStore(key);
        } finally {
            store.lock.readLock().unlock();
        }
    }

    @Override
    public void putKV(String key, String value) {
        final ConcurrentFileStore store = loadBalancer.balanceRequest(key, stores);
        try {
            store.lock.writeLock().lock();
            store.writeToStore(key, value, Tombstone.VALID);
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) {
        final ConcurrentFileStore store = loadBalancer.balanceRequest(key, stores);
        try {
            store.lock.writeLock().lock();
            store.writeToStore(key, "", Tombstone.DEAD);
        } finally {
            store.lock.writeLock().unlock();
        }
    }

    @Override
    public void clearStorage() {
        for (ConcurrentFileStore store : stores) {
            try {
                store.lock.writeLock().lock();
                new FileWriter(store.storage).close();
            } catch (IOException e) {
                logger.error("Could not clear storage", e);
            } finally {
                store.lock.writeLock().unlock();
            }
        }
    }

    enum Tombstone {
        VALID('V'),
        DEAD('D');

        final char marker;

        Tombstone(char marker) {
            this.marker = marker;
        }
    }

    /**
     * TODO: move out to its own KVStorage class, and add that keys index file?
     */
    static class ConcurrentFileStore {
        final ReadWriteLock lock;
        final File storage;

        public ConcurrentFileStore(String directory, String filename) {
            storage = new File(directory, filename);
            lock = new ReentrantReadWriteLock();

            try {
                //noinspection ResultOfMethodCallIgnored
                storage.getParentFile().mkdirs();
                if (storage.createNewFile()) logger.info(String.format("Store created: %s", storage.getName()));
                else logger.info(String.format("Store existing: %s", storage.getName()));
            } catch (IOException e) {
                throw new RuntimeException("Unable to create data stores", e);
            }
        }

        public String readFromStore(String key) {
            try (Stream<String> lines = Files.lines(storage.toPath())) {
                final String latestEntry = lines.filter(line -> key.equals(line.substring(1, line.indexOf(KV_DELIMITER))))
                        .reduce((a, b) -> b).orElse(null);
                return (latestEntry == null || latestEntry.charAt(0) != Tombstone.VALID.marker) ? null
                        : latestEntry.substring(latestEntry.indexOf(KV_DELIMITER) + 1);
            } catch (IOException e) {
                logger.error("An error occurred during read from store.", e);
                return null;
            }
        }

        public void writeToStore(String key, String value, Tombstone tombstone) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(storage, true))) {
                writer.println(tombstone.marker + key + KV_DELIMITER + value);
            } catch (IOException e) {
                logger.error("An error occurred during write to store.", e);
            }
        }

        /**
         * @return a load balancer, potentially with a micro-optimization if nodeCount is a power of two
         */
        public static ILoadBalancer<ConcurrentFileStore> createLoadBalancer(int nodeCount) {
            return ((nodeCount & (nodeCount - 1)) == 0) ? new PowerOfTwoLoadBalancer<>(nodeCount)
                    : (key, nodes) -> nodes.get(Math.abs(key.hashCode()) % nodeCount);
        }

        private static class PowerOfTwoLoadBalancer<T> implements ILoadBalancer<T> {
            final int mask;

            PowerOfTwoLoadBalancer(int nodeCount) {
                if ((nodeCount & (nodeCount - 1)) != 0)
                    throw new IllegalArgumentException("Node count must be a power of two");
                this.mask = nodeCount - 1;
            }

            @Override
            public T balanceRequest(String key, List<T> nodes) {
                return nodes.get(key.hashCode() & this.mask);
            }
        }
    }
}

