package testing;

import app_kvECS.ECSClient;
import app_kvHttp.model.request.Query;
import app_kvHttp.model.request.Remapping;
import app_kvServer.storage.IKVStorage;
import client.KVStore;
import ecs.IECSNode;
import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;
import shared.messages.KVMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ECSTests extends TestCase {

    private static ECSClient ecs;
    private static ZooKeeperService zk;

    static {
        try {
            // 1. Test init
            new LogSetup("logs/testing/test.log", Level.DEBUG);

            // 2. Setup ECS connection
            String filePath = "ecs.config", zkConnStr = ZooKeeperService.LOCALHOST_CONNSTR;
            ecs = new ECSClient(filePath, zkConnStr);
            zk = new ZooKeeperService(zkConnStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void tearDown() throws InterruptedException {
        ecs.shutdown();
        Thread.sleep(5000);
    }

    /**
     * Testing the send message protocol by mimicking a mock server
     *
     * @throws Exception
     */
    @Test
    public void testSendReceive() throws Exception {
        final String REPLY_TEXT = "Hello", TEST_NODE_NAME = "testSendReceive";
        final long TEST_TIMEOUT_MILLIS = 5000L;

        // Create a test server underneath zookeeper root
        final ZkECSNode node = new ZkECSNode(TEST_NODE_NAME, "127.0.0.1", 5001);
        zk.createNode(node.getZnode(), new KVAdminMessageProto(ECSClient.ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY), true);
        zk.watchDataOnce(node.getZnode(), () -> {
            try {
                KVAdminMessageProto reply = new KVAdminMessageProto(TEST_NODE_NAME, KVAdminMessage.AdminStatusType.EMPTY, REPLY_TEXT);
                zk.setData(node.getZnode(), reply.getBytes());
            } catch (IOException ignored) {
            } // test will time out and fail anyway
        });

        // Send a request to the dummy server and get a dummy response
        Future<KVAdminMessageProto> responseFuture = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                return node.sendMessage(zk, new KVAdminMessageProto(
                        ECSClient.ECS_NAME,
                        KVAdminMessage.AdminStatusType.EMPTY
                ), TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        });

        final KVAdminMessageProto response = responseFuture.get();
        assertNotNull(response);
        assertEquals(response.getValue(), REPLY_TEXT);
    }

    @Test
    public void testMultiServerGet() throws Exception {
        Collection<IECSNode> nodes = ecs.addNodes(3, "FIFO", 10);
        IECSNode coordinator = nodes.iterator().next();
        ecs.start();

        Thread.sleep(5000);

        final KVStore kvClient = new KVStore(coordinator.getNodeHost(), coordinator.getNodePort());
        kvClient.connect();

        nodes.forEach(node -> {
            try {
                kvClient.put(node.getNodeHost() + ":" + node.getNodePort(), "test" + node.getNodeName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        KVMessage res = kvClient.getAll(new Query(".*", ".*"));
        assertEquals(3, res.getValue().split("\n").length);
        kvClient.disconnect();
    }

    @Test
    public void testMultiServerPut() throws Exception {
        Collection<IECSNode> nodes = ecs.addNodes(3, "FIFO", 10);
        IECSNode coordinator = nodes.iterator().next();
        ecs.start();

        Thread.sleep(5000);

        final KVStore kvClient = new KVStore(coordinator.getNodeHost(), coordinator.getNodePort());
        kvClient.connect();

        nodes.forEach(node -> {
            try {
                kvClient.put(node.getNodeHost() + ":" + node.getNodePort(), "test");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        KVMessage res = kvClient.putAll(new Query(".*", ".*"), new Remapping("t", "T"));

        assertEquals(3, res.getValue().split("\n").length);

        List<IKVStorage.KVPair> updatedVals = res.getValue().lines()
                .map(IKVStorage.KVPair::deserialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        updatedVals.forEach(kv -> assertEquals("TesT", kv.value));
        kvClient.disconnect();
    }

    @Test
    public void testMultiServerDelete() throws Exception {
        Collection<IECSNode> nodes = ecs.addNodes(3, "FIFO", 10);
        IECSNode coordinator = nodes.iterator().next();
        ecs.start();

        Thread.sleep(5000);

        final KVStore kvClient = new KVStore(coordinator.getNodeHost(), coordinator.getNodePort());
        kvClient.connect();

        nodes.forEach(node -> {
            try {
                kvClient.put(node.getNodeHost() + ":" + node.getNodePort(), "test");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        kvClient.deleteAll(new Query(coordinator.getNodeHost() + ":" + coordinator.getNodePort(), ".*"));
        KVMessage res = kvClient.getAll(new Query(".*", ".*"));

        assertEquals(2, res.getValue().split("\n").length);
        kvClient.disconnect();
    }

    //3.  add node, start should see one node
    //4.  add nodes, start, should see multiple nodes
    //5. remove nodes
    //6.  add node, put, shutdown, get should succ -> GRACEFUL
    //7.  add node, put, delete, shutdown, get should fail -> GRACEFUL
    //8.  pgrep spin up node kill it check if its in zookeeper it shouldnt be (zkservice) -> FORCED
    //    example of pgrep for above is last function in ecs client
    //9.  Same as 7 except now check for key this is a test for replication -> FORCED
    //10.  pgrep spin up node kill check if same amount of zookeeper nodes crash recovery -> FORCED

    /**
     * Test addition of new server to an empty storage service
     */
    @Test
    public void testAddNode() throws Exception {
        IECSNode node = ecs.addNode("FIFO", 10);

        // assert a valid node was added
        assertNotNull(node);

        ecs.start();

        assertEquals(true, zk.nodeExists(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName()));

    }

    /**
     * Test addition of multiple servers to an empty storage service
     */
    @Test
    public void testAddNodes() throws Exception {
        Collection<IECSNode> nodesAdded = ecs.addNodes(4, "FIFO", 4);
        assertNotNull(nodesAdded);

        ecs.start();

        nodesAdded.forEach(node -> {
            try {
                assertTrue(zk.nodeExists(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    /**
     * Test removal of multiple nodes from storage service
     */
    @Test
    public void testRemoveNodes() throws Exception {
        Collection<IECSNode> nodesAdded = ecs.addNodes(4, "FIFO", 4);
        ecs.start();

        List<String> nodesToRemove = nodesAdded.stream().map(IECSNode::getNodeName).collect(Collectors.toList());
        ecs.removeNodes(nodesToRemove);
        Thread.sleep(1000);

        nodesAdded.forEach(node -> {
            try {
                assertFalse(zk.nodeExists(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Test data replication during graceful shutdown
     */
    @Test
    public void testGracefulReplication() throws Exception {

        IECSNode coordinator = ecs.addNode("FIFO", 4);
        final KVStore kvClient = new KVStore(coordinator.getNodeHost(), coordinator.getNodePort());

        ecs.start();
        kvClient.connect();

        kvClient.put("foo", "bar");

        Collection<IECSNode> replicas = ecs.addNodes(2, "FIFO", 4);
        ecs.start();

        Thread.sleep(15000);

        IECSNode firstReplica = replicas.iterator().next();

        kvClient.put(firstReplica.getNodeHost() + ":" + firstReplica.getNodePort(), "baz");
        Thread.sleep(5000);

        List<String> nodesToRemove = Arrays.asList(coordinator.getNodeName(), firstReplica.getNodeName());
        ecs.removeNodes(nodesToRemove);
        Thread.sleep(5000);


        assertEquals("bar", kvClient.get("foo").getValue());

        kvClient.disconnect();
    }

    /**
     * Tests graceful failure detection
     */
    @Test
    public void testGracefulFailureDetection() throws IOException, InterruptedException {
        IECSNode node = ecs.addNode("FIFO", 4);
        try {
            ecs.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(zk.nodeExists(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName()));
        CountDownLatch latch = new CountDownLatch(1);
        zk.watchDeletion(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName(), latch::countDown);

        ecs.removeNodes(Arrays.asList(node.getNodeName()));
        Thread.sleep(5000);

        assertTrue(latch.await(10000L, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests forced failure detection
     */
    @Test
    public void testForcedFailureDetection() throws IOException, InterruptedException {
        IECSNode node = ecs.addNode("FIFO", 4);
        try {
            ecs.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(zk.nodeExists(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName()));
        CountDownLatch latch = new CountDownLatch(1);
        zk.watchDeletion(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName(), latch::countDown);
        String script = "pkill -9 -f m4-server.jar";
        script = "ssh -n " + node.getNodeHost() + " nohup " + script + " &";
        Runtime run = Runtime.getRuntime();
        run.exec(script);
        assertTrue(latch.await(10000L, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests forced failure recovery i.e. new node spun up to replace failed node
     */
    @Test
    public void testForcedFailureRecovery() throws Exception {
        IECSNode node = ecs.addNode("FIFO", 4);

        ecs.start();

        Thread.sleep(5000);

        int numOriginalNodes = ecs.getNodes().size();

        CountDownLatch latch = new CountDownLatch(1);
        zk.watchDeletion(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName(), latch::countDown);

        String script = "pkill -f -9 " + node.getNodeName();
        script = "ssh -n " + node.getNodeHost() + " nohup " + script + " &";

        Runtime run = Runtime.getRuntime();
        run.exec(script);

        assertTrue(latch.await(10000L, TimeUnit.MILLISECONDS));

        Thread.sleep(5000);
        assertEquals(numOriginalNodes, ecs.getNodes().size());

    }

    /**
     * Test replication during forced failure
     */
    @Test
    public void testForcedReplication() throws Exception {
        IECSNode node = ecs.addNode("FIFO", 4);
        final KVStore kvClient = new KVStore(node.getNodeHost(), node.getNodePort());

        ecs.start();
        kvClient.connect();

        kvClient.put("foo", "bar");

        Collection<IECSNode> replicas = ecs.addNodes(2, "FIFO", 4);
        ecs.start();

        Thread.sleep(15000);

        IECSNode firstReplica = replicas.iterator().next();

        kvClient.put(firstReplica.getNodeHost() + ":" + firstReplica.getNodePort(), "baz");
        Thread.sleep(5000);

        CountDownLatch latch = new CountDownLatch(1);
        zk.watchDeletion(ZooKeeperService.ZK_SERVERS + "/" + node.getNodeName(), latch::countDown);

        String script = "pkill -9 -f " + node.getNodeName();
        script = "ssh -n " + node.getNodeHost() + " nohup " + script + " &";

        Runtime run = Runtime.getRuntime();
        run.exec(script);

        assertTrue(latch.await(10000L, TimeUnit.MILLISECONDS));
        Thread.sleep(5000);

        assertEquals("bar", kvClient.get("foo").getValue());

        kvClient.disconnect();
    }

}
