package app_kvServer.storage;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVNaiveStorage implements IKVStorage {
    private static final Logger logger = Logger.getRootLogger();

    private static final LoadBalancer loadBalancer = new LoadBalancer();

    private static final int NUM_PERSISTENT_STORES = 8;

    private static final List<ReadWriteLock> locks = new ArrayList<>();

    //TODO: change prints to logs

    public KVNaiveStorage() {
        for (int i = 0; i < NUM_PERSISTENT_STORES; i++) {
            try {
                String fileName = "data/store" + (i + 1) + ".txt";
                File store = new File(fileName);
                store.getParentFile().mkdirs();
                if (store.createNewFile()) {
                    logger.info("Store created: " + store.getName());
                }
            } catch (IOException e) {
                logger.error("An error occurred.", e);
            }

            locks.add(new ReentrantReadWriteLock());
        }
    }

    private String readFromStore(String key) {
        String value = null;
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        String fileName = "data/store" + (storeIndex + 1) + ".txt";
        try (Scanner reader = new Scanner(new File(fileName))) {
            String hashedKey = String.valueOf(key.hashCode());

            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                //TODO: check index returned by indexof but shouldn't have errors if writes correct
                int kvSeparatorIndex = line.indexOf("=");
                int flagIndex = line.lastIndexOf(",");
                String k = line.substring(0, kvSeparatorIndex);
                String flag = line.substring(flagIndex + 1);
                if (hashedKey.equals(k) && flag.equals("V")) {
                    value = line.substring(kvSeparatorIndex + 1, flagIndex);
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("An error occurred.", e);
        }
        return value;
    }

    private void writeToStore(String key, String value, boolean delete) {
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        String fileName = "data/store" + (storeIndex + 1) + ".txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true))) {

            String hashedKey = String.valueOf(key.hashCode());

            if (delete)
                bw.write(hashedKey + "=" + value + ",D");
            else
                bw.write(hashedKey + "=" + value + ",V");
            bw.newLine();
        } catch (IOException e) {
            logger.error("An error occurred.", e);
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
                logger.error("An error occurred.", e);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean inStorage(String key) throws Exception {
        return getKV(key) != null;
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
    public String putKV(String key, String value) throws Exception {
        String returnValue = getKV(key);
        int storeIndex = loadBalancer.getStoreIndex(key, NUM_PERSISTENT_STORES);
        Lock writeLock = locks.get(storeIndex).writeLock();
        try {
            writeLock.lock();
            writeToStore(key, value, false);
        } finally {
            writeLock.unlock();
        }
        return returnValue;
    }

    @Override
    public void deleteKV(String key) throws Exception {
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
        KVNaiveStorage helper = new KVNaiveStorage();
//        helper.putKV("hello", "world");
//        helper.putKV("ECE", "419");
//        helper.putKV("TEP", "322");
//        helper.putKV("ECE", "420");
//        helper.putKV("something", "new");
        System.out.println(helper.getKV("ECE"));
    }
}

//on hash look at everything before the = instead of string contains bc hash might contain another hash
//add a tombstone to mark as deleted
