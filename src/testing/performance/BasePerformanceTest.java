package testing.performance;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import app_kvServer.storage.IKVStorage.KVPair;
import client.KVStore;
import junit.framework.TestCase;
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

public class BasePerformanceTest extends TestCase {
    /**
     * NUM_UNIQ_REQS: the number of unique key/value pairs to generate
     * REQ_DUPLICITY: how many times each of the unique requests should be re-attempted
     * <p>
     * In total, the server will be hit with NUM_UNIQ_REQS * REQ_DUPLICITY * NUM_CLIENTS requests, though concurrency
     * and caching results may differ as you play around with the 3 vars
     */
    private static final int NUM_UNIQ_REQS = 100, REQ_DUPLICITY = 2;

    private static final List<KVPair> REQUEST_TEST_SET;
    private static KVServer SERVER;
    private static List<KVStore> CLIENTS;

    /*
     * Global set up
     */
    static {
        try {
            REQUEST_TEST_SET = generateTestSet();
        } catch (Exception e) {
            throw new RuntimeException("Could not generate test set", e);
        }
    }

    protected static List<KVPair> generateTestSet() {
        List<KVPair> allRequests = new ArrayList<>(NUM_UNIQ_REQS * REQ_DUPLICITY);
        List<KVPair> uniqueRequests = IntStream.range(0, NUM_UNIQ_REQS)
                .mapToObj(i -> new KVPair(generateRandomString(KVMessageProto.MAX_KEY_SIZE), generateRandomString(KVMessageProto.MAX_VALUE_SIZE)))
                .collect(Collectors.toList());
        for (int i = 0; i < REQ_DUPLICITY; i++) allRequests.addAll(uniqueRequests);
        return allRequests;
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

    /**
     * Per-test setup
     */
    @Override
    protected void setUp() throws Exception {
        SERVER = generateNewServer();
        CLIENTS = generateNewClients();

        for (KVStore kvClient : CLIENTS) kvClient.connect();
    }

    /**
     * Per-test teardown
     */
    @Override
    protected void tearDown() {
        for (KVStore kvClient : CLIENTS) kvClient.disconnect();
        SERVER.clearStorage();
        SERVER.close();
    }

    protected List<KVStore> generateNewClients() {
        final int NUM_CLIENTS = 8;

        return IntStream.range(0, NUM_CLIENTS)
                .mapToObj(i -> new KVStore("localhost", 50000))
                .collect(Collectors.toList());
    }

    protected KVServer generateNewServer() {
        final int CACHE_SIZE = 0;
        final IKVServer.CacheStrategy CACHE_STRATEGY = IKVServer.CacheStrategy.FIFO;

        return new KVServer(50000, CACHE_SIZE, CACHE_STRATEGY.toString());
    }

    public ThroughputResults singleClientPerformance(KVStore store, List<KVPair> tests, Predicate<Integer> isGetIteration) throws Exception {
        long getMsgSize = 0, getExecTime = 0, getCount = 0, putMsgSize = 0, putExecTime = 0, putCount = 0;

        List<String> gettableKeys = new LinkedList<>();
        Collections.shuffle(tests);

        int iterations = 0;
        for (KVPair test : tests) {
            iterations++;
            if (isGetIteration.test(iterations) && !gettableKeys.isEmpty()) {
                String key = gettableKeys.get(0);

                long start = System.currentTimeMillis();
                final KVMessage res = store.get(key);
                long finish = System.currentTimeMillis();
                assertNotSame("GET failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
                getCount++;
                getExecTime += (finish - start);
                getMsgSize += ((KVMessageProto) res).getByteRepresentation().length;
            } else {
                String key = test.key, value = test.value;

                long start = System.currentTimeMillis();
                final KVMessage res = store.put(key, value);
                long finish = System.currentTimeMillis();
                assertNotSame("PUT failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
                putCount++;
                putExecTime += (finish - start);
                putMsgSize += ((KVMessageProto) res).getByteRepresentation().length;

                gettableKeys.add(key);
                Collections.shuffle(gettableKeys);
            }
        }


        return new ThroughputResults(Thread.currentThread().getId(),
                getExecTime / (double) getCount,
                putExecTime / (double) putCount,
                getMsgSize / (double) getExecTime,
                putMsgSize / (double) putExecTime
        );
    }

    public void putGetPerformance(String testName, Predicate<Integer> isGetIteration) {
        final int NUM_CLIENTS = CLIENTS.size();
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Callable<ThroughputResults>> threads = CLIENTS.stream()
                .map(client -> (Callable<ThroughputResults>)
                        () -> singleClientPerformance(client, new ArrayList<>(REQUEST_TEST_SET), isGetIteration))
                .collect(Collectors.toList());

        try {
            // Since requests are evenly distributed among clients, sum of averages is equal to average of sums
            double averageClientLatencyGet = 0, averageTotalThroughputGet = 0, averageClientLatencyPut = 0, averageTotalThroughputPut = 0;
            for (Future<ThroughputResults> future : threadPool.invokeAll(threads)) {
                ThroughputResults result = future.get();
                averageClientLatencyGet += result.averageGetLatency / NUM_CLIENTS;
                averageClientLatencyPut += result.averagePutLatency / NUM_CLIENTS;
                averageTotalThroughputGet += result.averageGetThroughput;
                averageTotalThroughputPut += result.averagePutThroughput;
            }
            System.out.printf("%s | Server | Average GET Latency (ms) | %.3f\n", testName, averageClientLatencyGet);
            System.out.printf("%s | Server | Average GET Throughput (KB/s) | %.3f\n", testName, averageTotalThroughputGet);
            System.out.printf("%s | Server | Average PUT Latency (ms) | %.3f\n", testName, averageClientLatencyPut);
            System.out.printf("%s | Server | Average PUT Throughput (KB/s) | %.3f\n", testName, averageTotalThroughputPut);
        } catch (Exception e) {
            throw new RuntimeException("Threadpool encountered an error", e);
        }
    }

    @Test
    public void test10Get90PutPerformance() {
        putGetPerformance("10/90", i -> i % 10 == 0);
    }

    @Test
    public void test20Get80PutPerformance() {
        putGetPerformance("20/80", i -> i % 5 == 0);
    }

    @Test
    public void test30Get70PutPerformance() {
        putGetPerformance("30/70", i -> Arrays.asList(0, 1, 2).contains(i % 10));
    }

    @Test
    public void test40Get60PutPerformance() {
        putGetPerformance("40/60", i -> Arrays.asList(0, 1).contains(i % 5));
    }

    @Test
    public void test50Get50PutPerformance() {
        putGetPerformance("50/50", i -> i % 2 == 0);
    }

    @Test
    public void test60Get40PutPerformance() {
        putGetPerformance("60/40", i -> i % 5 > 1);
    }

    @Test
    public void test70Get30PutPerformance() {
        putGetPerformance("70/30", i -> i % 10 > 2);
    }

    @Test
    public void test80Get20PutPerformance() {
        putGetPerformance("80/20", i -> i % 5 > 0);
    }

    @Test
    public void test90Get10PutPerformance() {
        putGetPerformance("90/10", i -> i % 10 > 0);
    }

    private static class ThroughputResults {
        final long id;
        final double averageGetLatency, averagePutLatency, averageGetThroughput, averagePutThroughput;

        public ThroughputResults(long id, double averageGetLatency, double averagePutLatency, double averageGetThroughput, double averagePutThroughput) {
            this.id = id;
            this.averageGetLatency = averageGetLatency;
            this.averagePutLatency = averagePutLatency;
            this.averageGetThroughput = averageGetThroughput;
            this.averagePutThroughput = averagePutThroughput;
        }
    }
}
