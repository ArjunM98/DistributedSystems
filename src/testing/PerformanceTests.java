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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PerformanceTests extends TestCase {
    /**
     * NUM_UNIQ_REQS: the number of unique key/value pairs to generate
     * REQ_DUPLICITY: how many times each of the unique requests should be re-attempted
     * NUM_CLIENTS: number of clients (and threads) to spin up
     * <p>
     * In total, the server will be hit with NUM_UNIQ_REQS * REQ_DUPLICITY * NUM_CLIENTS requests, though concurrency
     * and caching results may differ as you play around with the 3 vars
     */
    private static final int NUM_UNIQ_REQS = 100, REQ_DUPLICITY = 1, NUM_CLIENTS = 8;

    private static KVServer SERVER;
    private static List<KVStore> CLIENTS;
    private static List<KeyValuePair> REQUEST_TEST_SET;

    static {
        try {
            // 1. Test init
            new LogSetup("logs/testing/test.log", Level.ERROR);

            REQUEST_TEST_SET = new ArrayList<>(NUM_UNIQ_REQS * REQ_DUPLICITY);
            List<KeyValuePair> uniqueRequests = IntStream.range(0, NUM_UNIQ_REQS)
                    .mapToObj(i -> new KeyValuePair(generateRandomString(KVStore.MAX_KEY_SIZE), generateRandomString(KVStore.MAX_VALUE_SIZE)))
                    .collect(Collectors.toList());
            REQUEST_TEST_SET.addAll(uniqueRequests);

            // 2. Client-server init
            SERVER = new KVServer(50000, 10, "FIFO");
            CLIENTS = new ArrayList<>(NUM_CLIENTS);
            for (int i = 0; i < NUM_CLIENTS; i++) CLIENTS.add(new KVStore("localhost", 50000));

            // 3. Start communications
            SERVER.start();
            for (KVStore kvClient : CLIENTS) kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateRandomString(int maxlength) {
        String alphaNumeric = String.join("", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz", "0123456789");

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

    public ThroughputResults singleClientPerformance(KVStore store, List<KeyValuePair> tests, Predicate<Integer> isGetIteration) throws Exception {
        int msgSize = 0, executionTime = 0, iterations = 0;

        Collections.shuffle(tests);
        for (KeyValuePair test : tests) {
            iterations++;
            String key = test.key, value = test.value;
            if (isGetIteration.test(iterations)) {
                msgSize += new KVMessageProto(KVMessage.StatusType.GET, key, "", iterations).getByteRepresentation().length;
                long start = System.currentTimeMillis();
                store.get(key);
                long finish = System.currentTimeMillis();
                executionTime += (finish - start);
            } else {
                msgSize += new KVMessageProto(KVMessage.StatusType.PUT, key, value, iterations).getByteRepresentation().length;
                long start = System.currentTimeMillis();
                store.put(key, value);
                long finish = System.currentTimeMillis();
                executionTime += (finish - start);
            }
        }


        return new ThroughputResults(Thread.currentThread().getId(),
                msgSize / (double) iterations,
                executionTime / (double) iterations,
                msgSize / (double) executionTime
        );
    }

    public void putGetPerformance(Predicate<Integer> isGetIteration) {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Callable<ThroughputResults>> threads = CLIENTS.stream()
                .map(client -> (Callable<ThroughputResults>)
                        () -> singleClientPerformance(client, new ArrayList<>(REQUEST_TEST_SET), isGetIteration))
                .collect(Collectors.toList());

        try {
            for (Future<ThroughputResults> future : threadPool.invokeAll(threads)) {
                ThroughputResults result = future.get();
                // TODO: decide whether we want to print per thread or aggregate and print global values
                System.out.printf("Thread %d: Average Message Size %.3f Bytes\n", result.id, result.averageMessageSize);
                System.out.printf("Thread %d: Average Latency %.3fms\n", result.id, result.averageLatency);
                System.out.printf("Thread %d: Average Throughput %.3f KB/s\n", result.id, result.averageThroughput);
            }
        } catch (Exception e) {
            throw new RuntimeException("Threadpool error");
        } finally {
            SERVER.clearCache();
            SERVER.clearStorage();
        }
    }

    @Test
    public void test20Get80PutPerformance() {
        System.out.println("20/80 Get-Put Ratio");
        putGetPerformance(i -> i % 5 != 0);
    }

    @Test
    public void test80Get20PutPerformance() {
        System.out.println("80/20 Get-Put Ratio");
        putGetPerformance(i -> i % 5 == 0);
    }

    @Test
    public void test50Get50PutPerformance() {
        System.out.println("50/50 Get-Put Ratio");
        putGetPerformance(i -> i % 2 == 0);
    }

    private static class KeyValuePair {
        final String key, value;

        KeyValuePair(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class ThroughputResults {
        final long id;
        final double averageMessageSize, averageLatency, averageThroughput;

        ThroughputResults(long id, double averageMessageSize, double averageLatency, double averageThroughput) {
            this.id = id;
            this.averageMessageSize = averageMessageSize;
            this.averageLatency = averageLatency;
            this.averageThroughput = averageThroughput;
        }
    }
}
