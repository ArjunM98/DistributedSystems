package testing;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.KVServer;
import app_kvServer.storage.IKVStorage.KVPair;
import client.KVStore;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.util.*;
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
    private static final int NUM_UNIQ_REQS = 100, REQ_DUPLICITY = 2, NUM_CLIENTS = 8;

    private static final int CACHE_SIZE = NUM_UNIQ_REQS;
    private static final CacheStrategy CACHE_STRATEGY = CacheStrategy.FIFO;

    private static KVServer SERVER;
    private static List<KVStore> CLIENTS;
    private static List<KVPair> REQUEST_TEST_SET;

    static {
        try {
            // 1. Test init
            new LogSetup("logs/testing/test.log", Level.ERROR);

            REQUEST_TEST_SET = new ArrayList<>(NUM_UNIQ_REQS * REQ_DUPLICITY);
            List<KVPair> uniqueRequests = IntStream.range(0, NUM_UNIQ_REQS)
                    .mapToObj(i -> new KVPair(generateRandomString(KVMessageProto.MAX_KEY_SIZE), generateRandomString(KVMessageProto.MAX_VALUE_SIZE)))
                    .collect(Collectors.toList());
            for (int i = 0; i < REQ_DUPLICITY; i++) REQUEST_TEST_SET.addAll(uniqueRequests);

            // 2. Client-server init
            SERVER = new KVServer(50000, "test", "", CACHE_SIZE, CACHE_STRATEGY.toString());
            CLIENTS = new ArrayList<>(NUM_CLIENTS);
            for (int i = 0; i < NUM_CLIENTS; i++) CLIENTS.add(new KVStore("localhost", 50000));

            // 3. Start communications
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

        for (int i = 0; i <= length; i++) {
            int index = random.nextInt(alphaNumeric.length());
            char randomChar = alphaNumeric.charAt(index);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    public ThroughputResults singleClientPerformance(KVStore store, List<KVPair> tests, Predicate<Integer> isGetIteration) throws Exception {
        int msgSize = 0, executionTime = 0, iterations = 0;

        Collections.shuffle(tests);
        for (KVPair test : tests) {
            iterations++;
            String key = test.key, value = test.value;
            if (isGetIteration.test(iterations)) {
                msgSize += new KVMessageProto(KVMessage.StatusType.GET, key, iterations).getByteRepresentation().length;
                long start = System.currentTimeMillis();
                final KVMessage res = store.get(key);
                long finish = System.currentTimeMillis();
                executionTime += (finish - start);
                assertNotSame("GET failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
            } else {
                msgSize += new KVMessageProto(KVMessage.StatusType.PUT, key, value, iterations).getByteRepresentation().length;
                long start = System.currentTimeMillis();
                final KVMessage res = store.put(key, value);
                long finish = System.currentTimeMillis();
                executionTime += (finish - start);
                assertNotSame("PUT failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
            }
        }


        return new ThroughputResults(Thread.currentThread().getId(),
                msgSize / (double) iterations,
                executionTime / (double) iterations,
                msgSize / (double) executionTime
        );
    }

    public void putGetPerformance(String testName, Predicate<Integer> isGetIteration) {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Callable<ThroughputResults>> threads = CLIENTS.stream()
                .map(client -> (Callable<ThroughputResults>)
                        () -> singleClientPerformance(client, new ArrayList<>(REQUEST_TEST_SET), isGetIteration))
                .collect(Collectors.toList());

        try {
            double serverAverageThroughput = 0; // since requests are evenly distributed among clients, sum of averages is equal to average of sums
            for (Future<ThroughputResults> future : threadPool.invokeAll(threads)) {
                ThroughputResults result = future.get();
                System.out.printf("%s | Thread %d | Average Message Size (KB) | %.3f\n", testName, result.id, result.averageMessageSize / 1000);
                System.out.printf("%s | Thread %d | Average Client Latency (ms) | %.3f\n", testName, result.id, result.averageLatency);
                System.out.printf("%s | Thread %d | Average Client Throughput (KB/s) | %.3f\n", testName, result.id, result.averageThroughput);
                serverAverageThroughput += result.averageThroughput;
            }
            System.out.printf("%s | Server | Average Server Throughput (KB/s) | %.3f\n", testName, serverAverageThroughput);
        } catch (Exception e) {
            throw new RuntimeException("Threadpool encountered an error", e);
        } finally {
            SERVER.clearCache();
            SERVER.clearStorage();
        }
    }

    @Test
    public void test10Get90PutPerformance() {
        System.out.println("10/90 Get-Put Ratio");
        putGetPerformance("10/90", i -> i % 10 == 0);
    }

    @Test
    public void test20Get80PutPerformance() {
        System.out.println("20/80 Get-Put Ratio");
        putGetPerformance("20/80", i -> i % 5 == 0);
    }

    @Test
    public void test30Get70PutPerformance() {
        System.out.println("30/70 Get-Put Ratio");
        putGetPerformance("30/70", i -> Arrays.asList(0, 1, 2).contains(i % 10));
    }

    @Test
    public void test40Get60PutPerformance() {
        System.out.println("40/60 Get-Put Ratio");
        putGetPerformance("40/60", i -> Arrays.asList(0, 1).contains(i % 5));
    }

    @Test
    public void test50Get50PutPerformance() {
        System.out.println("50/50 Get-Put Ratio");
        putGetPerformance("50/50", i -> i % 2 == 0);
    }

    @Test
    public void test60Get40PutPerformance() {
        System.out.println("60/40 Get-Put Ratio");
        putGetPerformance("60/40", i -> i % 5 > 1);
    }

    @Test
    public void test70Get30PutPerformance() {
        System.out.println("70/30 Get-Put Ratio");
        putGetPerformance("70/30", i -> i % 10 > 2);
    }

    @Test
    public void test80Get20PutPerformance() {
        System.out.println("80/20 Get-Put Ratio");
        putGetPerformance("80/20", i -> i % 5 > 0);
    }

    @Test
    public void test90Get10PutPerformance() {
        System.out.println("90/10 Get-Put Ratio");
        putGetPerformance("90/10", i -> i % 10 > 0);
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
