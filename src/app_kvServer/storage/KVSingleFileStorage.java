package app_kvServer.storage;

import app_kvServer.KVServerException;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private static final String KV_DELIMITER = " ";

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

    enum Tombstone {
        VALID('V'),
        DEAD('D');

        final char marker;

        Tombstone(char marker) {
            this.marker = marker;
        }
    }
}
