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

public abstract class BasePerformanceTest extends TestCase {
    /**
     * NUM_UNIQ_REQS: the number of unique key/value pairs to generate
     * REQ_DUPLICITY: how many times each of the unique requests should be re-attempted
     * <p>
     * In total, the server will be hit with NUM_UNIQ_REQS * REQ_DUPLICITY * NUM_CLIENTS requests, though concurrency
     * and caching results may differ as you play around with the 3 vars
     */
    protected static final int NUM_UNIQ_REQS = 100, REQ_DUPLICITY = 2;

    protected static final int CACHE_SIZE = NUM_UNIQ_REQS / 2;
    protected static final IKVServer.CacheStrategy CACHE_STRATEGY = IKVServer.CacheStrategy.FIFO;


    private static final List<KVPair> REQUEST_TEST_SET;
    private static List<KVServer> SERVERS; // TODO: replace with an ECS
    private static List<KVStore> CLIENTS;

    /*
     * Global set up
     */
    static {
        try {
            REQUEST_TEST_SET = generateTestSet();
            System.out.println(String.join(" | ",
                    "Clients",
                    "Servers",
                    "GET/Request Ratio",
                    "Average GET Latency (ms)",
                    "Average GET Throughput (MB/s)",
                    "Average PUT Latency (ms)",
                    "Average PUT Throughput (MB/s)"
            ));
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
        SERVERS = generateNewServers();
        CLIENTS = generateNewClients();

        for (KVStore kvClient : CLIENTS) kvClient.connect();
    }

    /**
     * Per-test teardown
     */
    @Override
    protected void tearDown() {
        for (KVStore kvClient : CLIENTS) kvClient.disconnect();
        for (KVServer kvServer : SERVERS) {
            kvServer.clearStorage();
            kvServer.close();
        }
    }

    protected abstract List<KVStore> generateNewClients();

    protected abstract List<KVServer> generateNewServers();

    protected abstract int getNumClients();

    protected abstract int getNumServers();

    public ThroughputResults singleClientPerformance(KVStore store, List<KVPair> tests, Predicate<Integer> isGetIteration) throws Exception {
        long getMsgSize = 0, getExecTime = 0, getCount = 0, putMsgSize = 0, putExecTime = 0, putCount = 0;

        Collections.shuffle(tests);
        tests = tests.subList(0, tests.size() / getNumClients());

        int iterations = 0;
        List<String> gettableKeys = new LinkedList<>();
        for (KVPair test : tests) {
            iterations++;
            if (isGetIteration.test(iterations) && !gettableKeys.isEmpty()) {
                String key = gettableKeys.get(0);

                long start = System.nanoTime();
                final KVMessage res = store.get(key);
                long finish = System.nanoTime();
                assertNotSame("GET failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
                getCount++;
                getExecTime += (finish - start);
                getMsgSize += ((KVMessageProto) res).getByteRepresentation().length / 1000;
            } else {
                String key = test.key, value = test.value;

                long start = System.nanoTime();
                final KVMessage res = store.put(key, value);
                long finish = System.nanoTime();
                assertNotSame("PUT failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
                putCount++;
                putExecTime += (finish - start);
                putMsgSize += ((KVMessageProto) res).getByteRepresentation().length / 1000;

                gettableKeys.add(key);
                Collections.shuffle(gettableKeys);
            }
        }

        return new ThroughputResults(Thread.currentThread().getId(),
                getCount,
                putCount,
                getExecTime / 1e6, // nanos->millis
                putExecTime / 1e6, // nanos->millis
                getMsgSize,
                putMsgSize
        );
    }

    public void putGetPerformance(String getRequestRatio, Predicate<Integer> isGetIteration) {
        final int NUM_CLIENTS = CLIENTS.size();
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Callable<ThroughputResults>> threads = CLIENTS.stream()
                .map(client -> (Callable<ThroughputResults>)
                        () -> singleClientPerformance(client, new ArrayList<>(REQUEST_TEST_SET), isGetIteration))
                .collect(Collectors.toList());

        try {
            // Since requests are evenly distributed among clients, sum of averages is equal to average of sums
            long totalGets = 0, totalPuts = 0;
            double totalGetsTime = 0, totalGetsBandwidth = 0, totalPutsTime = 0, totalPutsBandwidth = 0;
            for (Future<ThroughputResults> future : threadPool.invokeAll(threads)) {
                ThroughputResults result = future.get();
                totalGets += result.getsCount;
                totalPuts += result.putsCount;
                totalGetsTime += result.totalGetsTime;
                totalPutsTime += result.totalPutsTime;
                totalGetsBandwidth += result.totalGetsBandwidth;
                totalPutsBandwidth += result.totalPutsBandwidth;
            }

            System.out.printf("%d | %d | %s | %.3f | %.3f | %.3f | %.3f%n",
                    getNumClients(),
                    getNumServers(),
                    getRequestRatio,
                    (totalGetsTime / totalGets) / NUM_CLIENTS,
                    totalGetsBandwidth / (totalGetsTime / totalGets),
                    (totalPutsTime / totalPuts) / NUM_CLIENTS,
                    totalPutsBandwidth / (totalPutsTime / totalPuts)
            );
        } catch (Exception e) {
            throw new RuntimeException("Threadpool encountered an error", e);
        }
    }

    @Test
    public void test10Get90PutPerformance() {
        putGetPerformance("0.1", i -> i % 10 == 0);
    }

    @Test
    public void test20Get80PutPerformance() {
        putGetPerformance("0.2", i -> i % 5 == 0);
    }

    @Test
    public void test30Get70PutPerformance() {
        putGetPerformance("0.3", i -> Arrays.asList(0, 1, 2).contains(i % 10));
    }

    @Test
    public void test40Get60PutPerformance() {
        putGetPerformance("0.4", i -> Arrays.asList(0, 1).contains(i % 5));
    }

    @Test
    public void test50Get50PutPerformance() {
        putGetPerformance("0.5", i -> i % 2 == 0);
    }

    @Test
    public void test60Get40PutPerformance() {
        putGetPerformance("0.6", i -> i % 5 > 1);
    }

    @Test
    public void test70Get30PutPerformance() {
        putGetPerformance("0.7", i -> i % 10 > 2);
    }

    @Test
    public void test80Get20PutPerformance() {
        putGetPerformance("0.8", i -> i % 5 > 0);
    }

    @Test
    public void test90Get10PutPerformance() {
        putGetPerformance("0.9", i -> i % 10 > 0);
    }

    private static class ThroughputResults {
        final long id, getsCount, putsCount;
        final double totalGetsTime, totalPutsTime, totalGetsBandwidth, totalPutsBandwidth;

        public ThroughputResults(long id, long getsCount, long putsCount, double totalGetsTime, double totalPutsTime, double totalGetsBandwidth, double totalPutsBandwidth) {
            this.id = id;
            this.getsCount = getsCount;
            this.putsCount = putsCount;
            this.totalGetsTime = totalGetsTime;
            this.totalPutsTime = totalPutsTime;
            this.totalGetsBandwidth = totalGetsBandwidth;
            this.totalPutsBandwidth = totalPutsBandwidth;
        }
    }
}
