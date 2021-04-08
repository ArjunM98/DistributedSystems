package app_kvHttp.controller;

import app_kvHttp.model.Model;
import app_kvHttp.model.response.Status;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Objects;

public abstract class Handler implements HttpHandler {
    private static final Logger logger = Logger.getRootLogger();

    protected abstract ApiResponse execute(HttpExchange exchange) throws Exception;

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        logger.debug("Received request");

        ApiResponse response;
        try {
            response = Objects.requireNonNull(execute(exchange));
        } catch (Exception e) {
            logger.error("Unable to service request: " + e.getMessage());
            response = new ApiResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, new Status(KVMessage.StatusType.FAILED));
        }

        try (final OutputStream responseBody = exchange.getResponseBody()) {
            exchange.sendResponseHeaders(response.httpCode, response.body.length);
            responseBody.write(response.body);
        }
    }

    static class ApiResponse {
        /**
         * Like {@link HttpURLConnection#HTTP_OK}
         */
        public final int httpCode;

        /**
         * Raw response contents
         */
        public final byte[] body;

        ApiResponse(int httpCode, Object body) throws IOException {
            this.httpCode = httpCode;
            this.body = Model.toRaw(body);
        }
    }
}
