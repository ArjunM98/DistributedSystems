package app_kvServer.storage;

import app_kvServer.KVServerException;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A tombstone-based key-value store that enables fast writes and concurrent reads.
 * TODO: performance improvements (https://github.com/ArjunM98/DistributedSystems/issues/18)
 * - Key index to improve {@link #inStorage(String)} performance
 * - Tombstone compaction and/or other file cleanup when it gets too large
 */
public class KVSingleFileStorage implements IKVStorage {
    private static final Logger logger = Logger.getRootLogger();

    private final ReadWriteLock lock;
    private final File storage;

    public KVSingleFileStorage(String directory) {
        this(directory, "naive.txt");
    }

    public KVSingleFileStorage(String directory, String filename) {
        this.lock = new ReentrantReadWriteLock();
        this.storage = new File(directory, filename);

        //noinspection ResultOfMethodCallIgnored
        this.storage.getParentFile().mkdirs();
        try {
            if (this.storage.createNewFile()) logger.info(String.format("Store created: %s", this.storage.getName()));
            else logger.info(String.format("Store existing: %s", this.storage.getName()));
        } catch (IOException e) {
            throw new RuntimeException("Unable to create storage file", e);
        }
    }

    @Override
    public boolean inStorage(String key) {
        try {
            return getKV(key) != null;
        } catch (KVServerException e) {
            return false;
        }
    }

    @Override
    public String getKV(String key) throws KVServerException {
        try {
            lock.readLock().lock();
            return requireNonNull(readFromStore(key));
        } catch (Exception e) {
            throw new KVServerException("Key not found in storage", KVMessage.StatusType.GET_ERROR);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putKV(String key, String value) {
        try {
            lock.writeLock().lock();
            writeToStore(key, value, Tombstone.VALID);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws KVServerException {
        if (!inStorage(key)) throw new KVServerException("Key not found in storage", KVMessage.StatusType.DELETE_ERROR);
        try {
            lock.writeLock().lock();
            writeToStore(key, "", Tombstone.DEAD);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clearStorage() {
        try {
            lock.writeLock().lock();
            new FileWriter(storage).close();
        } catch (IOException e) {
            logger.error("Could not clear storage", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Stream<KVPair> openKvStream(Predicate<KVPair> filter) {
        try {
            lock.writeLock().lock();
            // 1. Remove dead entries; this will simplify the next steps since we can assume all keys are now unique
            compactTombstones();

            // 2. Copy over the desired keys into a new file
            final File tempStorage = new File(storage.getAbsolutePath() + ".tmp." + System.currentTimeMillis());
            try (Stream<String> inputLines = Files.lines(storage.toPath()); PrintWriter output = new PrintWriter(new FileWriter(tempStorage))) {
                inputLines.map(line -> new KVPair(line.substring(1, line.indexOf(KVPair.KV_DELIMITER)), line.substring(line.indexOf(KVPair.KV_DELIMITER) + 1)))
                        .filter(filter)
                        .forEach(kv -> output.println(kv.key + KVPair.KV_DELIMITER + kv.value));
            }

            // 3. Stream the results from this new file
            return Files.lines(tempStorage.toPath())
                    .onClose(() -> logger.info(tempStorage.delete()
                            ? "KV stream successfully closed"
                            : "Unable to delete temp file at " + tempStorage.getAbsolutePath()))
                    .map(KVPair::deserialize)
                    .filter(Objects::nonNull)
                    .filter(filter);
        } catch (IOException e) {
            logger.error("Could not retrieve KV pairs", e);
            return Stream.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * NOT thread-safe -- use an external ReadLock
     */
    private String readFromStore(String key) {
        try (Stream<String> lines = Files.lines(storage.toPath())) {
            final String latestEntry = lines.filter(line -> key.equals(line.substring(1, line.indexOf(KVPair.KV_DELIMITER))))
                    .reduce((a, b) -> b).orElse(null);
            return (latestEntry == null || latestEntry.charAt(0) != Tombstone.VALID.marker) ? null
                    : latestEntry.substring(latestEntry.indexOf(KVPair.KV_DELIMITER) + 1);
        } catch (IOException e) {
            logger.error("An error occurred during read from store.", e);
            return null;
        }
    }

    /**
     * NOT thread-safe -- use an external WriteLock
     */
    private void writeToStore(String key, String value, Tombstone tombstone) {
        try {
            if (!storage.canWrite() && !storage.createNewFile()) logger.error("Could not write to store");
            try (PrintWriter writer = new PrintWriter(new FileWriter(storage, true))) {
                writer.println(tombstone.marker + key + KVPair.KV_DELIMITER + value);
            }
        } catch (IOException e) {
            logger.error("An error occurred during write to store.", e);
        }
    }

    /**
     * NOT thread-safe -- use an external WriteLock
     *
     * @throws IOException if unable to complete compaction
     */
    private void compactTombstones() throws IOException {
        // 1. Build index of keys mapping to the row number of their most recent entry
        final Map<String, Integer> keyRowMap = new HashMap<>();

        try (Stream<String> lines = Files.lines(storage.toPath())) {
            final AtomicInteger index = new AtomicInteger(0);
            lines.sequential().forEachOrdered(line -> {
                final String key = line.substring(1, line.indexOf(KVPair.KV_DELIMITER));
                if (line.charAt(0) != Tombstone.VALID.marker) {
                    keyRowMap.remove(key);
                } else {
                    keyRowMap.put(key, index.get());
                }
                index.incrementAndGet();
            });
        }
        final Set<Integer> validRows = new HashSet<>(keyRowMap.values());

        // 2. Write valid keys over to a new file
        final File tempStorage = new File(storage.getAbsolutePath() + ".tmp." + System.currentTimeMillis());
        try (Stream<String> inputLines = Files.lines(storage.toPath()); PrintWriter output = new PrintWriter(new FileWriter(tempStorage))) {
            final AtomicInteger index = new AtomicInteger(0);
            inputLines.sequential().forEachOrdered(line -> {
                if (validRows.contains(index.getAndIncrement())) {
                    output.println(line);
                }
            });
        }

        // 3. Overwrite original file
        if (!storage.delete() || !tempStorage.renameTo(storage)) {
            throw new IOException("Unable to clear original file");
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
}
