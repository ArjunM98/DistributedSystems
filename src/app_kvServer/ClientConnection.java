package app_kvServer;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


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

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, KVServer server) {
        this.server = server;
        this.clientSocket = clientSocket;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try (clientSocket; InputStream input = clientSocket.getInputStream(); OutputStream output = clientSocket.getOutputStream()) {
            while (true) try {
                KVMessageProto request = new KVMessageProto(input);
                logger.debug("Responding to request " + request.getId());
                respondToRequest(request, output);
            } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
                String message = "Bad message: " + e.getMessage();
                logger.info(message, e);
                new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.ERROR_KEY, message, KVMessageProto.UNKNOWN_MESSAGE_ID)
                        .writeMessageTo(output); // TODO: Note this might throw again, in which case should we assume dead client?
            } catch (IOException | NullPointerException e) {
                logger.info("Client disconnected: " + e.getMessage());
                break;
            }
        } catch (IOException e) {
            logger.info("Unable to stream to/from socket", e);
        }
    }

    /**
     * Method receive a KVMessage using this socket.
     *
     * @throws IOException some I/O error regarding the input stream
     */
    private void respondToRequest(KVMessageProto req, OutputStream output) throws IOException {
        switch (req.getStatus()) {
            case GET:
                try {
                    String value = server.getKV(req.getKey());
                    new KVMessageProto(KVMessage.StatusType.GET_SUCCESS, req.getKey(), value, req.getId()).writeMessageTo(output);
                } catch (Exception e) {
                    new KVMessageProto(KVMessage.StatusType.GET_ERROR, req.getKey(), "", req.getId()).writeMessageTo(output);
                }
                break;
            case PUT:
                if ("null".equals(req.getValue())) try {
                    server.putKV(req.getKey(), req.getValue());
                    new KVMessageProto(KVMessage.StatusType.DELETE_SUCCESS, req.getKey(), req.getValue(), req.getId()).writeMessageTo(output);
                } catch (Exception e) {
                    new KVMessageProto(KVMessage.StatusType.DELETE_ERROR, req.getKey(), req.getValue(), req.getId()).writeMessageTo(output);
                }
                else try {
                    // TODO: concurrency bug here could cause two clients to both receive "PUT_SUCCESS"
                    KVMessage.StatusType putStatus = (server.inCache(req.getKey()) || server.inStorage(req.getKey()))
                            ? KVMessage.StatusType.PUT_UPDATE
                            : KVMessage.StatusType.PUT_SUCCESS;

                    server.putKV(req.getKey(), req.getValue());
                    new KVMessageProto(putStatus, req.getKey(), req.getValue(), req.getId()).writeMessageTo(output);
                } catch (Exception e) {
                    new KVMessageProto(KVMessage.StatusType.PUT_ERROR, req.getKey(), req.getValue(), req.getId()).writeMessageTo(output);
                }
                break;
            default:
                throw new IllegalArgumentException("Bad message");
        }
    }
}
