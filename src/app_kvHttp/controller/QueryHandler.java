package app_kvHttp.controller;

import app_kvHttp.model.Model;
import app_kvHttp.model.request.BodySelect;
import app_kvHttp.model.request.BodyUpdate;
import client.KVStore;
import client.KVStorePool;
import com.sun.net.httpserver.HttpExchange;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class QueryHandler extends Handler {
    public static final String PATH_PREFIX = "/api/query";
    private static final Pattern PATH_PREFIX_PATTERN = Pattern.compile("/*api/query/*");

    private final KVStorePool kvStorePool;

    public QueryHandler(KVStorePool kvStorePool) {
        this.kvStorePool = kvStorePool;
    }

    @Override
    protected ApiResponse execute(HttpExchange exchange) throws Exception {
        final String httpMethod = exchange.getRequestMethod();
        if ("POST".equalsIgnoreCase(httpMethod)) {
            final String operation = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("").split("/")[0];
            switch (operation.toUpperCase()) {
                case "GET":
                    return executeGet(exchange);
                case "DELETE":
                    return executeDelete(exchange);
                case "UPDATE":
                    return executeUpdate(exchange);
                default:
                    throw new UnsupportedOperationException(String.format("Illegal operation '%s'", operation));
            }
        }
        throw new UnsupportedOperationException(String.format("Unable to service method type '%s'", exchange.getRequestMethod()));
    }

    /**
     * POST /api/query/get
     */
    private ApiResponse executeGet(HttpExchange exchange) throws Exception {
        final BodySelect body = Model.fromRaw(exchange.getRequestBody(), BodySelect.class);
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.getAll(body.getFilter()));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }

    /**
     * POST /api/query/delete
     */
    private ApiResponse executeDelete(HttpExchange exchange) throws Exception {
        final BodySelect body = Model.fromRaw(exchange.getRequestBody(), BodySelect.class);
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.deleteAll(body.getFilter()));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }

    /**
     * POST /api/query/update
     */
    private ApiResponse executeUpdate(HttpExchange exchange) throws Exception {
        final BodyUpdate body = Model.fromRaw(exchange.getRequestBody(), BodyUpdate.class);
        final KVStore kvStore = kvStorePool.acquireResource(10, TimeUnit.SECONDS);
        try {
            return ApiResponse.fromKVMessage(kvStore.putAll(body.getFilter(), body.getMapping()));
        } finally {
            kvStorePool.releaseResource(kvStore);
        }
    }
}
