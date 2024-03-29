package app_kvServer;

import app_kvHttp.model.Model;
import app_kvHttp.model.request.Query;
import app_kvHttp.model.request.Remapping;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import shared.messages.KVMessage.StatusType;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.function.Consumer;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {
    private static final Logger logger = Logger.getRootLogger();

    private final Socket clientSocket; // TODO: consider potential hanging problem
    private final KVServer server;
    private final Consumer<ClientConnection> onDisconnect;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, KVServer server, Consumer<ClientConnection> onDisconnect) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.onDisconnect = onDisconnect;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try (clientSocket; InputStream input = clientSocket.getInputStream(); OutputStream output = clientSocket.getOutputStream()) {
            while (true) try {
                handleRequest(input).writeMessageTo(output);
            } catch (IOException e) {
                logger.info("Client disconnected: " + e.getMessage());
                break;
            }
        } catch (IOException e) {
            logger.info("Unable to stream to/from socket", e);
        }
        this.onDisconnect.accept(this);
    }

    /**
     * Close connection with client
     */
    public void close() {
        try {
            clientSocket.close();
        } catch (Exception e) {
            logger.warn("Unable to terminate connection with client: " + e.getMessage());
        }
    }

    /**
     * Method to handle receiving a KVMessage using this socket.
     *
     * @return KVMessageProto response to send to client
     * @throws IOException on client disconnected
     */
    private KVMessageProto handleRequest(InputStream input) throws IOException {
        KVMessageProto req;
        long reqId = KVMessageProto.UNKNOWN_MESSAGE_ID;
        try {
            try {
                req = new KVMessageProto(input);
                reqId = req.getId();
            } catch (InvalidProtocolBufferException e) {
                throw new KVServerException("Malformed request", StatusType.FAILED);
            } catch (IOException | NullPointerException e) {
                throw new IOException("Client disconnected", e);
            }

            logger.debug("Responding to request " + reqId + " on " + server.getPort());
            switch (req.getStatus()) {
                case GET_ALL:
                    return handleGetAll(req);
                case COORDINATE_GET_ALL:
                    return handleCoordinateGetAll(req);
                case PUT_ALL:
                    return handlePutAll(req);
                case COORDINATE_PUT_ALL:
                    return handleCoordinatePutAll(req);
                case DELETE_ALL:
                    return handleDeleteAll(req);
                case COORDINATE_DELETE_ALL:
                    return handleCoordinateDeleteAll(req);
                case GET:
                    return handleGet(req);
                case PUT:
                    return "null".equals(req.getValue()) ? handleDelete(req) : handlePut(req);
            }
            throw new KVServerException("Bad request type", StatusType.FAILED);
        } catch (KVServerException e) {
            logger.warn(String.format("Error processing request %d (%s): %s", reqId, e.getErrorCode(), e.getMessage()));
            return new KVMessageProto(
                    e.getErrorCode(),
                    KVMessageProto.SERVER_ERROR_KEY,
                    e.getErrorCode() == StatusType.SERVER_NOT_RESPONSIBLE ? server.getMetadata() : e.getMessage(),
                    reqId
            );
        }
    }

    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handleGet(KVMessageProto req) throws KVServerException {
        try {
            return new KVMessageProto(StatusType.GET_SUCCESS, req.getKey(), server.getKV(req.getKey()), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.GET_ERROR) throw e;
            return new KVMessageProto(StatusType.GET_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.GET_ERROR, req.getKey(), req.getId());
        }
    }

    /**
     * Helper function to handle a coordinator request for getting KV(s)
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error
     */
    private KVMessageProto handleCoordinateGetAll(KVMessageProto req) throws KVServerException {
        try {
            String val = server.coordinateGetAllKV(Model.fromString(req.getKey(), Query.class));
            return new KVMessageProto(StatusType.COORDINATE_GET_ALL_SUCCESS, req.getKey(), val, req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.COORDINATE_GET_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.COORDINATE_GET_ALL_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.COORDINATE_GET_ALL_ERROR, req.getKey(), req.getId());
        }
    }

    /**
     * Helper function to handle GET_ALL request
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error
     */
    private KVMessageProto handleGetAll(KVMessageProto req) throws KVServerException {
        try {
            String val = server.getAllKV(Model.fromString(req.getKey(), Query.class));
            return new KVMessageProto(StatusType.GET_ALL_SUCCESS, req.getKey(), val, req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.GET_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.GET_ALL_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.GET_ALL_ERROR, req.getKey(), req.getId());
        }
    }


    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handlePut(KVMessageProto req) throws KVServerException {
        try {
            // TODO: concurrency bug here could cause two clients to both receive "PUT_SUCCESS"
            final StatusType putStatus = (server.inCache(req.getKey()) || server.inStorage(req.getKey()))
                    ? StatusType.PUT_UPDATE
                    : StatusType.PUT_SUCCESS;

            server.putKV(req.getKey(), req.getValue());
            return new KVMessageProto(putStatus, req.getKey(), req.getValue(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.PUT_ERROR) throw e;
            return new KVMessageProto(e.getErrorCode(), req.getKey(), req.getValue(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.PUT_ERROR, req.getKey(), req.getValue(), req.getId());
        }
    }

    /**
     * Helper function to handle a coordinator request for updating KV(s)
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error
     */
    private KVMessageProto handleCoordinatePutAll(KVMessageProto req) throws KVServerException {
        try {
            String val = server.coordinatePutAllKV(Model.fromString(req.getKey(), Query.class), Model.fromString(req.getValue(), Remapping.class));
            return new KVMessageProto(StatusType.COORDINATE_PUT_ALL_SUCCESS, req.getKey(), val, req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.COORDINATE_PUT_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.COORDINATE_PUT_ALL_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.COORDINATE_PUT_ALL_ERROR, req.getKey(), req.getId());
        }
    }

    /**
     * Helper function to handle PUT_ALL request
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error
     */
    private KVMessageProto handlePutAll(KVMessageProto req) throws KVServerException {
        try {
            String val = server.putAllKV(Model.fromString(req.getKey(), Query.class), Model.fromString(req.getValue(), Remapping.class));
            return new KVMessageProto(StatusType.PUT_ALL_SUCCESS, req.getKey(), val, req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.PUT_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.PUT_ALL_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.PUT_ALL_ERROR, req.getKey(), req.getId());
        }
    }

    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handleDelete(KVMessageProto req) throws KVServerException {
        try {
            server.putKV(req.getKey(), req.getValue());
            return new KVMessageProto(StatusType.DELETE_SUCCESS, req.getKey(), req.getValue(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.DELETE_ERROR) throw e;
            return new KVMessageProto(StatusType.DELETE_ERROR, req.getKey(), req.getValue(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.DELETE_ERROR, req.getKey(), req.getValue(), req.getId());
        }
    }

    /**
     * Helper function to handle DELETE_ALL request
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handleDeleteAll(KVMessageProto req) throws KVServerException {
        try {
            server.deleteAll(Model.fromString(req.getKey(), Query.class));
            return new KVMessageProto(StatusType.DELETE_ALL_SUCCESS, req.getKey(), req.getValue(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.DELETE_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.DELETE_ALL_ERROR, req.getKey(), req.getValue(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.DELETE_ALL_ERROR, req.getKey(), req.getValue(), req.getId());
        }
    }

    /**
     * Helper function to handle a coordinator request for deleting KV(s)
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error
     */
    private KVMessageProto handleCoordinateDeleteAll(KVMessageProto req) throws KVServerException {
        try {
            server.coordinateDeleteAllKV(Model.fromString(req.getKey(), Query.class));
            return new KVMessageProto(StatusType.COORDINATE_DELETE_ALL_SUCCESS, req.getKey(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != StatusType.COORDINATE_DELETE_ALL_ERROR) throw e;
            return new KVMessageProto(StatusType.COORDINATE_DELETE_ALL_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(StatusType.COORDINATE_DELETE_ALL_ERROR, req.getKey(), req.getId());
        }
    }
}
