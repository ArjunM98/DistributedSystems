package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
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
        return new KVServer(port, cacheSize, strategy);
    }

    /*
     * Creates an ECSClient object for auto-testing purposes
     */
    public static IECSClient createECSClientObject(String path, String zooKeeperConnectionString) {
        return new ECSClient(path, zooKeeperConnectionString);
    }
}