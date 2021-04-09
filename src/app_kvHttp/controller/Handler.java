package app_kvHttp.controller;

import app_kvHttp.model.Model;
import app_kvHttp.model.response.KV;
import app_kvHttp.model.response.Status;
import app_kvServer.storage.IKVStorage;
import com.fasterxml.jackson.core.JacksonException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.stream.Collectors;

import static shared.messages.KVMessageProto.CLIENT_ERROR_KEY;

public abstract class Handler implements HttpHandler {
    private static final Logger logger = Logger.getRootLogger();

    protected abstract ApiResponse execute(HttpExchange exchange) throws Exception;

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        logger.debug(String.format("REQUEST: %s %s", exchange.getRequestMethod(), exchange.getRequestURI()));

        // 1. Service the request
        ApiResponse response;
        try {
            response = Objects.requireNonNull(execute(exchange));
        } catch (IllegalArgumentException | JacksonException e) {
            response = ApiResponse.badRequest(e);
        } catch (Exception e) {
            logger.error("Unable to service request: " + e);
            response = ApiResponse.serverError(e);
        }

        // 2. Send it to the client
        try (final OutputStream responseBody = exchange.getResponseBody()) {
            exchange.getResponseHeaders().set("Content-Type", String.format("application/json; charset=%s", Charset.defaultCharset()));
            exchange.sendResponseHeaders(response.httpCode, response.body.length);
            responseBody.write(response.body);
        }
        logger.debug(String.format("RESPONSE [%d]: %s %s", response.httpCode, exchange.getRequestMethod(), exchange.getRequestURI()));
    }

    static class ApiResponse {
        /**
         * Default error messages
         */
        private static final String
                BAD_REQUEST_DEFAULT_MESSAGE = "Malformed request",
                NOT_FOUND_DEFAULT_MESSAGE = "Content not found",
                SERVER_ERROR_DEFAULT_MESSAGE = "Unknown failure";

        /**
         * Like {@link HttpURLConnection#HTTP_OK}
         */
        public final int httpCode;

        /**
         * Raw response contents
         */
        public final byte[] body;

        private ApiResponse(int httpCode, Object body) throws IOException {
            this.httpCode = httpCode;
            this.body = Model.toRaw(body);
        }

        static ApiResponse of(int httpCode, Object body) {
            try {
                return new ApiResponse(httpCode, body);
            } catch (IOException e) {
                throw new RuntimeException("Unable to create API response");
            }
        }

        static ApiResponse badRequest(Object... message) {
            final Object details = (message == null || message.length < 1) ? BAD_REQUEST_DEFAULT_MESSAGE : message[0];
            return ApiResponse.of(HttpURLConnection.HTTP_BAD_REQUEST, new Status(KVMessage.StatusType.FAILED, details));
        }

        static ApiResponse notFound(Object... message) {
            final Object details = (message == null || message.length < 1) ? NOT_FOUND_DEFAULT_MESSAGE : message[0];
            return ApiResponse.of(HttpURLConnection.HTTP_NOT_FOUND, new Status(KVMessage.StatusType.FAILED, details));
        }

        static ApiResponse serverError(Object... message) {
            final Object details = (message == null || message.length < 1) ? SERVER_ERROR_DEFAULT_MESSAGE : message[0];
            return ApiResponse.of(HttpURLConnection.HTTP_INTERNAL_ERROR, new Status(KVMessage.StatusType.FAILED, details));
        }

        static ApiResponse fromKVMessage(KVMessage kvMessage) {
            final Status status = new Status(kvMessage.getStatus(), kvMessage.getValue());
            switch (status.getStatus()) {
                case GET_SUCCESS:
                case PUT_UPDATE:
                case DELETE_SUCCESS:
                    // 200
                    return ApiResponse.of(HttpURLConnection.HTTP_OK, new KV(kvMessage.getKey(), kvMessage.getValue()));
                case PUT_SUCCESS:
                    // 201
                    return ApiResponse.of(HttpURLConnection.HTTP_CREATED, new KV(kvMessage.getKey(), kvMessage.getValue()));
                case COORDINATE_GET_ALL_SUCCESS:
                    // 200
                    return ApiResponse.of(HttpURLConnection.HTTP_OK, kvMessage.getValue().lines()
                            .map(IKVStorage.KVPair::deserialize)
                            .filter(Objects::nonNull)
                            .map(kv -> new KV(kv.key, kv.value))
                            .collect(Collectors.toList()));
                case COORDINATE_PUT_ALL_SUCCESS:
                    // 201
                    return ApiResponse.of(HttpURLConnection.HTTP_CREATED, kvMessage.getValue().lines()
                            .map(IKVStorage.KVPair::deserialize)
                            .filter(Objects::nonNull)
                            .map(kv -> new KV(kv.key, kv.value))
                            .collect(Collectors.toList()));
                case COORDINATE_DELETE_ALL_SUCCESS:
                    // 200
                    return ApiResponse.of(HttpURLConnection.HTTP_OK, status);
                case GET_ERROR:
                case PUT_ERROR:
                case DELETE_ERROR:
                case COORDINATE_GET_ALL_ERROR:
                case COORDINATE_PUT_ALL_ERROR:
                case COORDINATE_DELETE_ALL_ERROR:
                    // 404
                    return ApiResponse.of(HttpURLConnection.HTTP_NOT_FOUND, status);
                case FAILED:
                    // 400 or 500
                    if (CLIENT_ERROR_KEY.equals(kvMessage.getKey())) return ApiResponse.badRequest(status.getMessage());
                    else return ApiResponse.serverError(status.getMessage());
                case SERVER_WRITE_LOCK:
                case SERVER_STOPPED:
                    // 503
                    return ApiResponse.of(HttpURLConnection.HTTP_UNAVAILABLE, status);
            }
            return ApiResponse.serverError();
        }
    }

    /**
     * Convenience class to generate consistent 404 messages
     */
    public static class NotFoundHandler extends Handler {
        @Override
        protected ApiResponse execute(HttpExchange exchange) throws Exception {
            return ApiResponse.notFound();
        }
    }
}
