package testing;

import app_kvECS.ECSClient;
import app_kvHttp.KVHttpService;
import app_kvHttp.model.Model;
import app_kvHttp.model.request.BodySelect;
import app_kvHttp.model.request.BodyUpdate;
import client.KVStore;
import ecs.IECSNode;
import ecs.ZkECSNode;
import ecs.zk.ZooKeeperService;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import java.util.Map;

import static shared.messages.KVMessage.StatusType.*;

public class HttpTests extends TestCase {
    private static final int HTTP_PORT = 8000;
    private static final String BASE_URL = String.format("http://localhost:%d", HTTP_PORT);

    private static ECSClient ECS;
    private static KVStore KV_STORE;
    private static KVHttpService HTTP_SERVER;

    static {
        try {
            // 1. Test init
            new LogSetup("logs/testing/test.log", Level.ERROR);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Per-test setup
     */
    @Before
    public void setUp() throws Exception {
        String filePath = "ecs.config", zkConnStr = ZooKeeperService.LOCALHOST_CONNSTR;
        // 1. Set up ECS
        ECS = new ECSClient(filePath, zkConnStr);
        ECS.addNodes(3, "FIFO", 10);

        boolean started = ECS.start();
        if (!started) {
            throw new Exception("Unable to start all specified servers");
        }

        // 2. Set up a KV store connection
        final IECSNode kvServer = ECS.getNodes().values().iterator().next();
        KV_STORE = new KVStore(kvServer.getNodeHost(), kvServer.getNodePort());
        KV_STORE.connect();

        // 3. Set up HTTP
        HTTP_SERVER = new KVHttpService(HTTP_PORT, zkConnStr);
    }

    /**
     * Per-test teardown
     */
    @After
    public void tearDown() throws IOException {
        HTTP_SERVER.close();
        KV_STORE.disconnect();

        ECS.shutdown();
        // Shutdown is ack-ed right away, but needs some time to complete
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ECS.close();
    }

    @Test
    public void testHttpClientError() throws Exception {
        final String invalidKey = "this_key_is_longer_than_twenty_bytes";

        // 1. Test 404
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(String.format("%s/this/path/does/not/exist", BASE_URL)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 404", HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());

        // 2. Test 400
        response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, invalidKey)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 400", HttpURLConnection.HTTP_BAD_REQUEST, response.statusCode());
    }

    @Test
    public void testHttpServerError() throws Exception {
        final String key = "key";

        // 1. Remove all nodes from ECS
        final Map<String, IECSNode> originalServers = ECS.getNodes();
        assertTrue("Serverside error test setup failed", ECS.removeNodes(originalServers.keySet()));

        // 2. Test 500
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 500", HttpURLConnection.HTTP_INTERNAL_ERROR, response.statusCode());

        // 3. Restore ECS service
        final ZkECSNode node = ((ZkECSNode) originalServers.values().iterator().next());
        ECS.addNodes(originalServers.size(), node.getNodeCacheStrategy(), node.getNodeCacheSize());
    }

    @Test
    public void testBasicGet() throws Exception {
        final String key = "key", value = "value";

        // 1. Ensure key exists
        KVMessage res = KV_STORE.put(key, value);
        assertTrue("GET test setup failed", EnumSet.of(PUT_SUCCESS, PUT_UPDATE).contains(res.getStatus()));

        // 2. GET it
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 200", HttpURLConnection.HTTP_OK, response.statusCode());

        // 3. Ensure key does not exist
        res = KV_STORE.put(key, null);
        assertEquals("GET test setup failed", DELETE_SUCCESS, res.getStatus());

        // 4. GET it
        response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .GET()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 404", HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    @Test
    public void testBasicPut() throws Exception {
        final String key = "key", value = "value", newValue = "new_value";

        // 1. Ensure key does not exist
        KVMessage res = KV_STORE.put(key, null);
        assertTrue("PUT test setup failed", EnumSet.of(DELETE_SUCCESS, DELETE_ERROR).contains(res.getStatus()));

        // 2. PUT it
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .PUT(HttpRequest.BodyPublishers.ofString(value))
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 201 (created)", HttpURLConnection.HTTP_CREATED, response.statusCode());

        // 3. PUT it again
        response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .PUT(HttpRequest.BodyPublishers.ofString(newValue))
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 200", HttpURLConnection.HTTP_OK, response.statusCode());

        // 4. Make sure it's there
        res = KV_STORE.get(key);
        assertEquals("PUT test did not update", GET_SUCCESS, res.getStatus());
    }

    @Test
    public void testBasicDelete() throws Exception {
        final String key = "key", value = "value";

        // 1. Ensure key exists
        KVMessage res = KV_STORE.put(key, value);
        assertTrue("DELETE test setup failed", EnumSet.of(PUT_SUCCESS, PUT_UPDATE).contains(res.getStatus()));

        // 2. DELETE it
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .DELETE()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 200", HttpURLConnection.HTTP_OK, response.statusCode());

        // 3. DELETE it again
        response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder()
                        .DELETE()
                        .uri(URI.create(String.format("%s/api/kv/%s", BASE_URL, key)))
                        .build(),
                HttpResponse.BodyHandlers.ofString()
        );
        assertEquals("Response should be 404", HttpURLConnection.HTTP_NOT_FOUND, response.statusCode());
    }

    @Test
    public void testQueryBodyParser() {
        final String
                fullQuery = "{\"filter\": {\"keyFilter\": \"regex\",\"valueFilter\": \"regex\"}}",
                incompleteQuery = "{\"filter\": {}}",
                invalidQuery = "{\"filter\": {\"keyFilter\": \"[unclosed regex\",\"valueFilter\": \"regex\"}}";

        // 1. Test valid query
        BodySelect query = null;
        try {
            query = Model.fromString(fullQuery, BodySelect.class);
        } catch (Exception ignored) {
        }
        assertNotNull("Full query should be valid", query);

        // 2. Test incomplete query
        query = null;
        try {
            query = Model.fromString(incompleteQuery, BodySelect.class);
        } catch (Exception ignored) {
        }
        assertNull("Incomplete query should fail", query);

        // 3. Test invalid regex
        query = null;
        try {
            query = Model.fromString(invalidQuery, BodySelect.class);
        } catch (Exception ignored) {
        }
        assertNull("Invalid query should fail", query);
    }

    @Test
    public void testRemappingBodyParser() {
        final String
                fullRemapping = "{\"filter\": {\"keyFilter\": \"regex\",\"valueFilter\": \"regex\"},\"mapping\": {\"find\": \"regex\",\"replace\": \"replacement\"}}",
                incompleteRemapping = "{\"filter\": {\"keyFilter\": \"regex\",\"valueFilter\": \"regex\"},\"mapping\": {}}",
                invalidRemapping = "{\"filter\": {\"keyFilter\": \"regex\",\"valueFilter\": \"regex\"},\"mapping\": {\"find\": \"[unclosed regex\",\"replace\": \"replacement\"}}";

        // 1. Test valid remapping
        BodyUpdate remapping = null;
        try {
            remapping = Model.fromString(fullRemapping, BodyUpdate.class);
        } catch (Exception ignored) {
        }
        assertNotNull("Full remapping should be valid", remapping);

        // 2. Test incomplete remapping
        remapping = null;
        try {
            remapping = Model.fromString(incompleteRemapping, BodyUpdate.class);
        } catch (Exception ignored) {
        }
        assertNull("Incomplete remapping should fail", remapping);

        // 3. Test invalid regex
        remapping = null;
        try {
            remapping = Model.fromString(invalidRemapping, BodyUpdate.class);
        } catch (Exception ignored) {
        }
        assertNull("Invalid remapping should fail", remapping);
    }
}
