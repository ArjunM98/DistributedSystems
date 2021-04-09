package testing.performance;

import app_kvECS.ECSClient;
import app_kvHttp.model.request.Query;
import app_kvServer.IKVServer;
import app_kvServer.storage.IKVStorage.KVPair;
import client.KVStore;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class QueryScalePerformanceTest extends TestCase {
    /**
     * ENRON_MAIL_DIR: path to the *UNCOMPRESSED* dataset
     * ENRON_SUBSET_MAILBOX: path to a mailbox within the dataset to use for our tests
     */
    private static final String ENRON_MAIL_DIR = "enron_mail_20150507",
            ENRON_SUBSET_MAILBOX = "dasovich-j/all_documents";

    /**
     * NUM_UNIQ_REQS: the number of unique key/value pairs to generate
     * REQ_DUPLICITY: how many times each of the unique requests should be re-attempted
     * <p>
     * In total, the server will be hit with NUM_UNIQ_REQS * REQ_DUPLICITY * NUM_CLIENTS requests, though concurrency
     * and caching results may differ as you play around with the 3 vars
     */
    protected static final int NUM_UNIQ_REQS = 10, REQ_DUPLICITY = 1, NUM_CLIENTS = 20;

    protected static final int CACHE_SIZE = NUM_UNIQ_REQS / 2;
    protected static final IKVServer.CacheStrategy CACHE_STRATEGY = IKVServer.CacheStrategy.FIFO;


    private static final List<KVPair> REQUEST_TEST_SET;
    private static ECSClient ECS;
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

    /**
     * Generate a KVPair test set from a subset of the enron mail dataset
     */
    protected static List<KVPair> generateTestSet() {
        List<KVPair> allRequests = new ArrayList<>(NUM_UNIQ_REQS * REQ_DUPLICITY);
        File emailDirectory = new File(ENRON_MAIL_DIR, ENRON_SUBSET_MAILBOX);

        List<KVPair> uniqueRequests = Arrays.asList(Objects.requireNonNull(emailDirectory.listFiles()))
                .subList(0, NUM_UNIQ_REQS)
                .stream()
                .map(file -> {
                    String key = file.getName(), value;
                    try (Stream<String> lines = Files.lines(file.toPath())) {
                        value = String.join("+", lines.toArray(String[]::new));
                        if (value.length() > KVMessageProto.MAX_VALUE_SIZE) {
                            value = value.substring(0, KVMessageProto.MAX_VALUE_SIZE);
                        }
                    } catch (Exception e) {
                        return null;
                    }
                    return new KVPair(key, value);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        for (int i = 0; i < REQ_DUPLICITY; i++) allRequests.addAll(uniqueRequests);
        return allRequests;
    }

    /**
     * Per-test setup
     */
    @Before
    public void setUp() throws Exception {
        ECS = generateNewServers();
        final IECSNode node = ECS.getNodes().values().iterator().next();
        CLIENTS = generateNewClients(node.getNodeHost(), node.getNodePort());

        ECS.addNodes(1, CACHE_STRATEGY.toString(), CACHE_SIZE);
        boolean started = ECS.start();
        if (!started) {
            throw new Exception("Unable to start all specified servers");
        }
        for (KVStore kvClient : CLIENTS) kvClient.connect();
    }

    /**
     * Per-test teardown
     */
    @After
    public void tearDown() throws IOException {
        for (KVStore kvClient : CLIENTS) kvClient.disconnect();
        ECS.shutdown();
        // Shutdown is ack-ed right away, but needs some time to complete
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ECS.close();
    }

    protected List<KVStore> generateNewClients(String hostname, int port) {
        return IntStream.range(0, NUM_CLIENTS)
                .mapToObj(i -> new KVStore(hostname, port))
                .collect(Collectors.toList());
    }

    protected ECSClient generateNewServers() {
        ECSClient ecsClient;
        try {
            final String TEMP_FILE_NAME = "ecs.tmp.config";
            try (PrintWriter writer = new PrintWriter(new FileWriter(TEMP_FILE_NAME))) {
                Stream.of(
                        "server1 ug132 50000",
                        "server2 ug133 50000",
                        "server3 ug134 50000",
                        "server4 ug135 50000",
                        "server5 ug136 50000",
                        "server6 ug137 50000",
                        "server7 ug138 50000",
                        "server8 ug139 50000",
                        "server9 ug140 50000",
                        "server10 ug141 50000"
                ).forEach(writer::println);
            }
            ecsClient = new ECSClient(TEMP_FILE_NAME, ZooKeeperService.LOCALHOST_CONNSTR);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create ECS", e);
        }

        return ecsClient;
    }

    @Test
    public void testScaling() throws Exception {
        // Populate server
        final ExecutorService threadPool = Executors.newFixedThreadPool(NUM_CLIENTS);

        AtomicLong totalSize = new AtomicLong(0);
        IntStream.range(0, NUM_CLIENTS).forEach(i -> threadPool.submit(() -> {
            try {
                final KVStore client = CLIENTS.get(i);
                final List<KVPair> tests = REQUEST_TEST_SET.subList(i * NUM_CLIENTS, (i + 1) * NUM_CLIENTS);
                for (KVPair test : tests) {
                    final KVMessage res = client.put(test.key, test.value);
                    totalSize.addAndGet(test.serialize().getBytes(StandardCharsets.UTF_8).length);
                    assertNotSame("PUT failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
                    assertNotSame("PUT failed: " + res, KVMessage.StatusType.SERVER_STOPPED, res.getStatus());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(300, TimeUnit.SECONDS));

        for (int i = 1; i < 6; i++) {
            long start;
            // TEST HTTP query: get*
            start = System.nanoTime();
            KVMessage res = CLIENTS.get(0).getAll(new Query(".*", ".*"));
            assertNotSame("GET failed: " + res, KVMessage.StatusType.FAILED, res.getStatus());
            System.out.printf("Size %d: query took %.3f ms%n", i, (System.nanoTime() - start) / 1e6);

            // TEST SCALING: add node
            ECS.addNode("FIFO", 10);
            start = System.nanoTime();
            ECS.start();
            System.out.printf("Addition of one node took %.3f ms%n", (System.nanoTime() - start) / 1e6);
        }
    }
}