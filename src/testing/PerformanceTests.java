package testing;

import app_kvServer.KVServer;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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

    public void putGetPerformance(Predicate<Integer> isGetIteration) throws Exception {
        int msgSize = 0, executionTime = 0, iterations = 0;

        for (int rep = 0; rep < 3; rep++) {
            Collections.shuffle(KVPairs);
            for (String[] kvPair : KVPairs) {
                iterations++;
                String key = kvPair[0], value = kvPair[1];
                if (isGetIteration.test(iterations)) {
                    msgSize += new KVMessageProto(KVMessage.StatusType.GET, key, "", iterations).getByteRepresentation().length;
                    long start = System.currentTimeMillis();
                    kvClient.get(key);
                    long finish = System.currentTimeMillis();
                    executionTime += (finish - start);
                } else {
                    msgSize += new KVMessageProto(KVMessage.StatusType.PUT, key, value, iterations).getByteRepresentation().length;
                    long start = System.currentTimeMillis();
                    kvClient.put(key, value);
                    long finish = System.currentTimeMillis();
                    executionTime += (finish - start);
                }
            }
        }

        double avgMsgSize = msgSize / (double) iterations,
                avgExecutionTime = executionTime / (double) iterations,
                avgThroughput = avgMsgSize / avgExecutionTime;

        System.out.printf("Average Message Size %f Bytes\n", avgMsgSize);
        System.out.printf("Average Execution Time %f MilliSeconds\n", avgExecutionTime);
        System.out.printf("Average Throughput %f KB/Second\n", avgThroughput);

        cleanUpServer();
    }

    @Test
    public void test20Get80PutPerformance() throws Exception {
        System.out.println("20/80 Get-Put Ratio");
        putGetPerformance(i -> i % 5 != 0);
    }

    @Test
    public void test80Get20PutPerformance() throws Exception {
        System.out.println("80/20 Get-Put Ratio");
        putGetPerformance(i -> i % 5 == 0);
    }

    @Test
    public void test50Get50PutPerformance() throws Exception {
        System.out.println("50/50 Get-Put Ratio");
        putGetPerformance(i -> i % 2 == 0);
    }
}
