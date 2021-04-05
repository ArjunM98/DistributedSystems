package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import app_kvServer.ECSServerConnection;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.zk.ZooKeeperService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class ObjectFactory {
    private static final Logger logger = Logger.getRootLogger();

    /*
     * Creates a KVClient object for auto-testing purposes
     */
    public static IKVClient createKVClientObject() {
        return new KVClient();
    }

    /*
     * Creates a KVServer object for auto-testing purposes
     */
    public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
        logger.warn("Note: this method is not M2-compliant, please use the other one. This doesn't play well with ZK.");
        try {
            final ZooKeeperService zk = new ZooKeeperService(ZooKeeperService.LOCALHOST_CONNSTR);
            zk.initializeRootNodesForEcs(ECSClient.ECS_NAME);
            zk.setData(ZooKeeperService.ZK_METADATA, ECSHashRing.fromConfig(
                    String.format("test localhost %d", port), ECSNode::fromConfig
            ).toConfig().getBytes(StandardCharsets.UTF_8));
            final KVServer server = new KVServer(port, "test", ZooKeeperService.LOCALHOST_CONNSTR, cacheSize, strategy);
            server.updateServerState(ECSServerConnection.State.STARTED);
            return server;
        } catch (IOException e) {
            throw new RuntimeException("Unable to construct KVServer", e);
        }
    }

    /*
     * Creates a KVServer object for auto-testing purposes
     */
    public static IKVServer createKVServerObject(int port, String name, String connectionString, int cacheSize, String strategy) {
        logger.info("Note: the created server will be in STOPPED state");
        return new KVServer(port, name, connectionString, cacheSize, strategy);
    }

    /*
     * Creates an ECSClient object for auto-testing purposes
     */
    public static IECSClient createECSClientObject(String path, String zooKeeperConnectionString) {
        try {
            return new ECSClient(path, zooKeeperConnectionString);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create an IECSClient", e);
        }
    }
}