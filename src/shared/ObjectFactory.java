package shared;

import app_kvClient.IKVClient;
import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

import java.io.IOException;

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
    public static IKVServer createKVServerObject(int port, String name, String connectionString, int cacheSize, String strategy) {
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