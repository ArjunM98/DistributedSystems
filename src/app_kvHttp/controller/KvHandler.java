package app_kvHttp.controller;

import com.sun.net.httpserver.HttpExchange;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

public class KvHandler extends Handler {
    private static final Logger logger = Logger.getRootLogger();
    public static final String PATH_PREFIX = "/api/kv";
    private static final Pattern PATH_PREFIX_PATTERN = Pattern.compile("/*api/kv/*");

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
    private ApiResponse executeGet(HttpExchange exchange) {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        logger.info("Would get: " + key);
        // TODO: KVStore.get(key)
        return null;
    }

    /**
     * /api/kv/{key}
     */
    private ApiResponse executeDelete(HttpExchange exchange) {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        logger.info("Would delete: " + key);
        // TODO: KVStore.put(key, null)
        return null;
    }

    /**
     * /api/kv/{key}
     */
    private ApiResponse executePut(HttpExchange exchange) throws IOException {
        final String key = PATH_PREFIX_PATTERN.matcher(exchange.getRequestURI().getPath()).replaceFirst("");
        final String value = new String(exchange.getRequestBody().readAllBytes());
        logger.info("Would put: " + key + " / " + value);
        // TODO: KVStore.put(key, value)
        return null;
    }
}
