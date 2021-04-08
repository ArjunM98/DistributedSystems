package app_kvHttp.controller;

import app_kvHttp.model.Model;
import app_kvHttp.model.request.BodySelect;
import app_kvHttp.model.request.BodyUpdate;
import app_kvHttp.model.request.Query;
import app_kvHttp.model.request.Remapping;
import com.sun.net.httpserver.HttpExchange;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

public class QueryHandler extends Handler {
    private static final Logger logger = Logger.getRootLogger();
    public static final String PATH_PREFIX = "/api/query";
    private static final Pattern PATH_PREFIX_PATTERN = Pattern.compile("/*api/query/*");

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
     * /api/query/get
     */
    private ApiResponse executeGet(HttpExchange exchange) throws IOException {
        final Query filter = Model.fromRaw(exchange.getRequestBody(), BodySelect.class).getFilter();
        logger.info("Would get: " + new String(Model.toRaw(filter)));
        // TODO: KVStore.getAll(filter.getKeyFilter(), filter.getValueFilter())
        return null;
    }

    /**
     * /api/query/delete
     */
    private ApiResponse executeDelete(HttpExchange exchange) throws IOException {
        final Query filter = Model.fromRaw(exchange.getRequestBody(), BodySelect.class).getFilter();
        logger.info("Would delete: " + new String(Model.toRaw(filter)));
        // TODO: KVStore.deleteAll(filter.getKeyFilter(), filter.getValueFilter())
        return null;
    }

    /**
     * /api/query/update
     */
    private ApiResponse executeUpdate(HttpExchange exchange) throws IOException {
        final BodyUpdate bodyUpdate = Model.fromRaw(exchange.getRequestBody(), BodyUpdate.class);
        final Query filter = bodyUpdate.getFilter();
        final Remapping mapping = bodyUpdate.getMapping();
        logger.info("Would put: " + new String(Model.toRaw(filter)) + " / " + new String(Model.toRaw(mapping)));
        // TODO: KVStore.putAll(filter.getKeyFilter(), filter.getValueFilter(), mapping.getValue())
        return null;
    }
}
