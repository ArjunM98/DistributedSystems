package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import shared.messages.KVMessage;
import shared.messages.KVMessageProto;
import shared.messages.ProtoKVMessage;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private final Socket clientSocket;
    private final KVServer server;
    private boolean isOpen;

    private InputStream input;
    private OutputStream output;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, KVServer server) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while(isOpen) {
                try {
                    respondToRequest();

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            // TODO Put in appropriate logger
            System.out.println(ioe);
        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                // TODO Put in appropriate logger
                System.out.println("Error! Unable to tear down connection!");
            }
        }
    }

    /**
     * Method receive a KVMessage using this socket.
     * @throws IOException some I/O error regarding the input stream
     */
    private void respondToRequest() throws IOException {
        // TODO Implement parsing based on message status and better error handling
        KVMessageProto req = new KVMessageProto(input);
        KVMessageProto res;

        if (!req.validKVProto()) {
            return;
        }

        switch(req.getStatus()) {
            case GET:
                res = new KVMessageProto(KVMessage.StatusType.GET_SUCCESS, req.getKey(), req.getValue(), req.getId());
                break;
            case PUT:
                res = new KVMessageProto(KVMessage.StatusType.PUT_SUCCESS, req.getKey(), req.getValue(), req.getId());
                break;
            default:
                res = new KVMessageProto(KVMessage.StatusType.GET_ERROR, req.getKey(), req.getValue(), req.getId());
        }

        res.writeMessageTo(output);
    }


}
