package testing;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

public class PerformanceTests extends TestCase {

    private static KVStore kvClient;
    private static KVServer kvServer;
    private static final int COMMANDS = 100;
    private static List<String[]> KVPairs;

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            kvServer = new KVServer(50000, 10, "FIFO");
            kvClient = new KVStore("localhost", 50000);
            generateRandomKV();
            kvServer.start();
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public static String generateRandomString(int maxlength) {
        String upperAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerAlphabet = "abcdefghijklmnopqrstuvwxyz";
        String numbers = "0123456789";
        String alphaNumeric = upperAlphabet + lowerAlphabet + numbers;

        StringBuilder sb = new StringBuilder();
        Random random = new Random();

        int length = random.nextInt(maxlength);

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(alphaNumeric.length());
            char randomChar = alphaNumeric.charAt(index);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    public static void generateRandomKV() {
        KVPairs = new ArrayList<>(COMMANDS);
        for (int i = 0; i < COMMANDS; i++) {
            KVPairs.add(new String[]{generateRandomString(KVStore.MAX_KEY_SIZE), generateRandomString(KVStore.MAX_VALUE_SIZE)});
        }
    }

    public void cleanUpServer() {
        kvServer.clearCache();
        kvServer.clearStorage();
    }

    public void putGetPerformance(Predicate <Integer> get) throws Exception {
        Collections.shuffle(KVPairs);
        long msgSize = 0;
        long executionTime = 0;

        for(int rep = 0; rep < 100; rep++) {
            for (int i = 0; i < COMMANDS; i++) {
                String key = KVPairs.get(i)[0];
                String value = KVPairs.get(i)[1];
                if (get.test(i)) {
                    msgSize += new KVMessageProto(KVMessage.StatusType.GET, key, "", i).getByteRepresentation().length;
                    long start = System.currentTimeMillis();
                    kvClient.get(key);
                    long finish = System.currentTimeMillis();
                    executionTime += (finish - start);
                } else {
                    msgSize += new KVMessageProto(KVMessage.StatusType.PUT, key, value, i).getByteRepresentation().length;
                    long start = System.currentTimeMillis();
                    kvClient.put(key, value);
                    long finish = System.currentTimeMillis();
                    executionTime += (finish - start);
                }
            }
        }

        double avgMsgSize = msgSize / (double) COMMANDS;
        double avgExecutionTime = executionTime / (double) COMMANDS;
        double avgThroughput = avgMsgSize / avgExecutionTime;

        System.out.printf("Average Message Size %f Bytes\n", avgMsgSize);
        System.out.printf("Average Execution Time %f MilliSeconds\n", avgExecutionTime);
        System.out.printf("Average Throughput %f KB/Second\n", avgThroughput);

        cleanUpServer();
    }


    @Test
    public void test2080PutGetPerformance() throws Exception {
        System.out.println("20/80 Get-Put Ratio");
        putGetPerformance(i -> i % 5 != 0);
    }

    @Test
    public void test8020PutGetPerformance() throws Exception {
        System.out.println("80/20 Get-Put Ratio");
        putGetPerformance(i -> i % 5 == 0);
    }

    @Test
    public void test5050PutGetPerformance() throws Exception {
        System.out.println("50/50 Get-Put Ratio");
        putGetPerformance(i -> i % 2 == 0);
    }
}
