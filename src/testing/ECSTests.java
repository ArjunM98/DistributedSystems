package testing;

import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.zk.ZooKeeperService;
import ecs.zkwatcher.ECSMessageResponseWatcher;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ECSTests extends TestCase {

    private static ECS ecs;
    private static ZooKeeperService zk;

    static {
        try {
            // 1. Test init
            new LogSetup("logs/testing/test.log", Level.DEBUG);

            // 2. Setup ECS connection
            String filePath = "ecs.config";
            ecs = new ECS(filePath);
            zk = new ZooKeeperService(ECS.ZK_HOST + ":" + ECS.ZK_PORT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * To keep it independent of server response. The following calls setup directly
     */
    @Test
    public void testAddNodes() {
        Collection<IECSNode> nodesToAdd = ecs.setupNodes(5, "FIFO", 10);
        assertEquals(nodesToAdd.size(), 5);
    }

    @Test
    public void testSendReceive() throws Exception {
        // Create a test server underneath zookeeper root
        zk.createNode(ECS.ZK_SERVERS + "/" + "testSendReceive", new KVAdminMessageProto(ECS.ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY), true);
        zk.getData(ECS.ZK_SERVERS + "/" + "testSendReceive", watchedEvent -> {
            KVAdminMessageProto sevRes = new KVAdminMessageProto(ECS.ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY, "Hello");
            try {
                zk.setData(ECS.ZK_SERVERS + "/" + "testSendReceive", sevRes.getBytes());
            } catch (KeeperException | InterruptedException e) {
            }
        });

        // Send a request to the dummy server and get a dummy response
        Future<KVAdminMessageProto> response = Executors.newSingleThreadExecutor().invokeAll((Collections.singletonList((Callable<KVAdminMessageProto>) () -> {
            KVAdminMessageProto req = new KVAdminMessageProto(ECS.ECS_NAME, KVAdminMessage.AdminStatusType.EMPTY);
            ECSMessageResponseWatcher lis = new ECSMessageResponseWatcher(zk, new ECSNode("testSendReceive", "127.0.0.1", 5001));
            try {
                return lis.sendMessage(req, 5000, TimeUnit.MILLISECONDS);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }))).get(0);

        assertEquals(response.get(5, TimeUnit.SECONDS).getValue(), "Hello");
    }
}
