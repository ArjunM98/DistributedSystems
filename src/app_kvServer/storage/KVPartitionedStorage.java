package app_kvServer.storage;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVPartitionedStorage implements IKVStorage {
    private static final Logger logger = Logger.getRootLogger();

    private static final LoadBalancer loadBalancer = new LoadBalancer();

    private static final int NUM_PERSISTENT_STORES = 8;

    private static final List<ReadWriteLock> locks = new ArrayList<>();

    public KVPartitionedStorage() {
        for (int i = 0; i < NUM_PERSISTENT_STORES; i++) {
            try {
                String fileName = "data/store" + (i + 1) + ".txt";
                File store = new File(fileName);
                //noinspection ResultOfMethodCallIgnored
                store.getParentFile().mkdirs();
                if (store.createNewFile()) {
                    logger.info("Store created: " + store.getName());
                }
            } catch (IOException e) {
                logger.error("An error occurred during store creation.", e);
            }

            locks.add(new ReentrantReadWriteLock());
        }
    }

    private String readFromStore(String key) {
        String value = null;
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        String fileName = "data/store" + (storeIndex + 1) + ".txt";
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int kvSeparatorIndex = line.indexOf(" ");
                int flagIndex = line.lastIndexOf(",");
                String k = line.substring(0, kvSeparatorIndex);
                String flag = line.substring(flagIndex + 1);
                if (key.equals(k) && flag.equals("V")) {
                    value = line.substring(kvSeparatorIndex + 1, flagIndex);
                } else if (key.equals(k) && flag.equals("D")) {
                    value = null;
                }
            }
        } catch (IOException e) {
            logger.error("An error occurred during read from store.", e);
        }
        return value;
    }

    private void writeToStore(String key, String value, boolean delete) {
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        String fileName = "data/store" + (storeIndex + 1) + ".txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true))) {

            if (delete)
                bw.write(key + " " + value + ",D");
            else
                bw.write(key + " " + value + ",V");
            bw.newLine();
        } catch (IOException e) {
            logger.error("An error occurred during write to store.", e);
        }

    }

    private void clearStore(int storeIndex) {
        Lock writeLock = locks.get(storeIndex).writeLock();
        try {
            writeLock.lock();
            String fileName = "data/store" + (storeIndex + 1) + ".txt";
            try (FileWriter writer = new FileWriter(fileName)) {
                writer.write("");
            } catch (IOException e) {
                logger.error("An error occurred during clearing of store.", e);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean inStorage(String key) {
        try {
            return getKV(key) != null;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getKV(String key) throws Exception {
        String value;
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        Lock readLock = locks.get(storeIndex).readLock();
        try {
            readLock.lock();
            value = readFromStore(key);
        } finally {
            readLock.unlock();
        }
        return value;
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        Lock writeLock = locks.get(storeIndex).writeLock();
        try {
            writeLock.lock();
            writeToStore(key, value, false);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete(String key) throws Exception {
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        Lock writeLock = locks.get(storeIndex).writeLock();
        try {
            writeLock.lock();
            writeToStore(key, "", true);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clearStorage() {
        for (int i = 0; i < NUM_PERSISTENT_STORES; i++) {
            clearStore(i);
        }
    }

    public static void main(String[] args) throws Exception {
        new LogSetup("logs/kvserver.log", Level.ALL);
        KVPartitionedStorage helper = new KVPartitionedStorage();
        System.out.println(helper.getKV("ECE"));
        helper.putKV("ECE", "419");
        System.out.println(helper.getKV("ECE"));
        helper.delete("ECE");
        System.out.println(helper.getKV("ECE"));
    }
}

//on hash look at everything before the = instead of string contains bc hash might contain another hash
//add a tombstone to mark as deleted
