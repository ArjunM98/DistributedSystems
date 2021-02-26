package testing;

import app_kvECS.ECSClient;
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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ECSTests extends TestCase {

    private static ECSClient ecs;
    private static ZooKeeperService zk;
    private static KVStore kvClient;

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

    /**
     * Test addition of new server to an empty storage service
     */
    @Test
    public void testAddNodes() {
        IECSNode node = ecs.addNode("FIFO", 10);

        // assert a valid node was added
        assertNotNull(node);
        System.out.printf("Added %s to the queued storage service%n", node.getNodeName());
    }

}
