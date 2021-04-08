package app_kvHttp.controller;

import client.KVStore;
import client.KVStorePool;
import com.sun.net.httpserver.HttpExchange;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class KvHandler extends Handler {
    public static final String PATH_PREFIX = "/api/kv";
    private static final Pattern PATH_PREFIX_PATTERN = Pattern.compile("/*api/kv/*");

    private final KVStorePool kvStorePool;

    public KvHandler(KVStorePool kvStorePool) {
        this.kvStorePool = kvStorePool;
    }

    @Override
    protected ApiResponse execute(HttpExchange exchange) throws Exception {
        final String httpMethod = exchange.getRequestMethod();
        switch (httpMethod.toUpperCase()) {
            case "GET":
                return executeGet(exchange);
            case "DELETE":
                return executeDelete(exchange);
            case "PUT":
                return executePut(exchange);
            default:
                throw new UnsupportedOperationException(String.format("Unable to service method type '%s'", exchange.getRequestMethod()));
        }
    }

    /**
     * /api/kv/{key}
     */
    private ApiResponse executeGet(HttpExchange exchange) throws Exception {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.get(key));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }

    /**
     * /api/kv/{key}
     */
    private ApiResponse executeDelete(HttpExchange exchange) throws Exception {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.put(key, null));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }

    /**
     * /api/kv/{key}
     */
    private ApiResponse executePut(HttpExchange exchange) throws Exception {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        final String value = new String(exchange.getRequestBody().readAllBytes());
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.put(key, value));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }
}
