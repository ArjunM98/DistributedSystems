package app_kvServer;

import app_kvServer.storage.IKVStorage;
import app_kvServer.IKVServer.State;
import com.google.protobuf.InvalidProtocolBufferException;
import ecs.ECSHashRing;
import org.apache.log4j.Logger;
import shared.messages.KVAdminMessage;
import shared.messages.KVAdminMessageProto;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ECSServerConnection {
    private static final Logger logger = Logger.getRootLogger();

    private final KVServer server;
    private String connectedHost;

    /**
     * Constructs a new ECS Server Connection object for a given node.
     *
     * @param server the server for which to maintain the connection.
     */
    public ECSServerConnection(KVServer server) {
        this.server = server;
        this.connectedHost = null;
    }

    /**
     * Method to handle receiving a KVMessage using this socket.
     *
     * @return KVMessageProto response to send to client
     * @throws IOException on client disconnected
     */
    private KVAdminMessageProto handleRequest(byte[] input) throws IOException {
        KVAdminMessageProto req;
        try {
            try {
                req = new KVAdminMessageProto(input);
            } catch (InvalidProtocolBufferException e) {
                throw new KVServerException("Malformed request", KVAdminMessage.AdminStatusType.FAILED);
            }

            logger.debug("Responding to request on " + server.getPort());
            switch (req.getStatus()) {
                case START:
                    return handleStart();
                case STOP:
                    return handleStop();
                case SHUTDOWN:
                    return handleShutdown();
                case LOCK:
                    return handleLock();
                case UNLOCK:
                    return handleUnlock();
                case MOVE_DATA:
                    return handleMove(req);
                case TRANSFER_REQ:
                    return handleTransfer();
                case TRANSFER_BEGIN:
                    return handleTransferBegin(req);
            }
            throw new KVServerException("Bad request type", KVAdminMessage.AdminStatusType.FAILED);
        } catch (KVServerException e) {
            logger.warn(String.format("Error processing request (%s): %s", e.getErrorCode(), e.getMessage()));
            return new KVAdminMessageProto(
                    server.getName(),
                    KVAdminMessage.AdminStatusType.ERROR,
                    e.getMessage()
            );
        }
    }

    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link KVMessage.StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handleGet(KVMessageProto req) throws KVServerException {
        try {
            return new KVMessageProto(KVMessage.StatusType.GET_SUCCESS, req.getKey(), server.getKV(req.getKey()), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != KVMessage.StatusType.GET_ERROR) throw e;
            return new KVMessageProto(KVMessage.StatusType.GET_ERROR, req.getKey(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(KVMessage.StatusType.GET_ERROR, req.getKey(), req.getId());
        }
    }

    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link KVMessage.StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handlePut(KVMessageProto req) throws KVServerException {
        try {
            // TODO: concurrency bug here could cause two clients to both receive "PUT_SUCCESS"
            final KVMessage.StatusType putStatus = (server.inCache(req.getKey()) || server.inStorage(req.getKey()))
                    ? KVMessage.StatusType.PUT_UPDATE
                    : KVMessage.StatusType.PUT_SUCCESS;

            server.putKV(req.getKey(), req.getValue());
            return new KVMessageProto(putStatus, req.getKey(), req.getValue(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != KVMessage.StatusType.PUT_ERROR) throw e;
            return new KVMessageProto(KVMessage.StatusType.PUT_ERROR, req.getKey(), req.getValue(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(KVMessage.StatusType.PUT_ERROR, req.getKey(), req.getValue(), req.getId());
        }
    }

    /**
     * Helps clean up {@link #handleRequest(InputStream)}
     *
     * @param req request to process
     * @return KVMessageProto response to send to client
     * @throws KVServerException to communicate an expected general error (e.g. {@link KVMessage.StatusType#SERVER_NOT_RESPONSIBLE})
     */
    private KVMessageProto handleDelete(KVMessageProto req) throws KVServerException {
        try {
            server.putKV(req.getKey(), req.getValue());
            return new KVMessageProto(KVMessage.StatusType.DELETE_SUCCESS, req.getKey(), req.getValue(), req.getId());
        } catch (KVServerException e) {
            if (e.getErrorCode() != KVMessage.StatusType.DELETE_ERROR) throw e;
            return new KVMessageProto(KVMessage.StatusType.DELETE_ERROR, req.getKey(), req.getValue(), req.getId());
        } catch (Exception e) {
            return new KVMessageProto(KVMessage.StatusType.DELETE_ERROR, req.getKey(), req.getValue(), req.getId());
        }
    }


    private KVAdminMessageProto handleStart() {
        server.setServerState(State.STARTED);
        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.START_ACK);
    }

    private KVAdminMessageProto handleStop() {
        server.setServerState(State.STOPPED);
        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.STOP_ACK);
    }

    private KVAdminMessageProto handleShutdown() {
        server.close();
        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.SHUTDOWN_ACK);
    }

    private KVAdminMessageProto handleLock()  {
        //if server is locked just return the ack
        server.setServerState(State.LOCKED);
        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.LOCK_ACK);
    }

    private KVAdminMessageProto handleUnlock() {
        server.setServerState(State.UNLOCKED);
        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.UNLOCK_ACK);
    }

    private KVAdminMessageProto handleTransfer() throws IOException {

        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
        } catch (IOException e) {
            throw(e);
        }

        int portNum = socket.getLocalPort();
        ServerSocket finalSocket = socket;
        new Thread(){
            @Override
            public void run() {
                try {
                    Socket IOSocket = finalSocket.accept();
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(IOSocket.getInputStream()))) {
                        server.putAllFromKvStream(in.lines());
                    } catch (IOException e) {
                        logger.error("Error occurred during data transfer", e);
                    }
                } catch (IOException e) {
                    logger.error("Error occurred during data receive", e);
                }
            }
        }.start();

        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.MOVE_DATA_ACK, Integer.toString(portNum));
    }

    //just open port
    private KVAdminMessageProto handleMove(KVAdminMessageProto req) throws IOException {
        Socket IOSocket = null;
        //TODO: figure out how to parse this maybe helper method
        String hostName = "";
        int portNum = 0;
        //transferring


        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.TRANSFER_REQ_ACK);
    }

    //TODO: ask Arjun KVAdminMessageProto has one with range, address but if we're stateless
    //TODO: wouldn't those need to be split into one with only range and one with only address?
    private KVAdminMessageProto handleTransferBegin(KVAdminMessageProto req) {
        String[] range = req.getRange();
        // TODO: helper method to parse range, need to know min, max, l, r
        BigInteger l = BigInteger.valueOf(0);
        BigInteger r = BigInteger.valueOf(0);
        BigInteger min = BigInteger.valueOf(0);
        BigInteger max = BigInteger.valueOf(0);

        Predicate<IKVStorage.KVPair> filter = kvPair -> {
            BigInteger hashVal = ECSHashRing.computeHash(kvPair.key);
            //comparison could be one-lined but ugly
            if (l.compareTo(r) <= 0) {
                return hashVal.compareTo(l) >= 0 && hashVal.compareTo(r) <= 0;
            } else {
                return (hashVal.compareTo(l) >= 0 && hashVal.compareTo(max) <= 0) || (hashVal.compareTo(min) >= 0 && hashVal.compareTo(r) <= 0);
            }
        };

        Socket IOSocket = null;
        try {
            IOSocket = new Socket(hostName, portNum);
        } catch (IOException e) {
            throw(e);
        }

        //TRANSFERRING CASE
        try (Stream<String> s = server.openKvStream(filter);
             PrintWriter out = new PrintWriter(IOSocket.getOutputStream(), true)) {
            s.forEach(out::println);
        } catch (IOException e) {
            logger.error("Error occurred during data transfer", e);
        }

        return new KVAdminMessageProto(server.getName(), KVAdminMessage.AdminStatusType.TRANSFER_COMPLETE);
    }

}
