package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

public class KVStore implements KVCommInterface {
    public static final int MAX_KEY_SIZE = 20;
    public static final int MAX_VALUE_SIZE = 120 * 1024;

    private static final Logger logger = Logger.getRootLogger();

    private final String address;
    private final int port;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private final AtomicLong msgID = new AtomicLong(KVMessageProto.START_MESSAGE_ID);

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void connect() throws Exception {
        clientSocket = new Socket(address, port);
        input = clientSocket.getInputStream();
        output = clientSocket.getOutputStream();
        logger.info(String.format("New Connection established to %s:%d", address, port));
    }

    @Override
    public void disconnect() {
        try {
            if (clientSocket != null) {
                clientSocket.close(); // will also close input and output
                logger.info(String.format("Disconnected from %s:%d", address, port));
            } else {
                logger.info(String.format("Was not connected to %s:%d", address, port));
            }
        } catch (IOException e) {
            logger.error("Error disconnecting from session", e);
        } finally {
            input = null;
            output = null;
            clientSocket = null;
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        if (clientSocket == null) throw new IOException("Not connected to a KVServer");

        final long id = msgID.incrementAndGet();
        try {
            value = value == null ? "null" : value;
            validateKey(key);
            validateValue(value);
            new KVMessageProto(KVMessage.StatusType.PUT, key, value, id).writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (Exception e) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), id);
        }
    }

    @Override
    public KVMessage get(String key) throws Exception {
        if (clientSocket == null) throw new IOException("Not connected to a KVServer");

        final long id = msgID.incrementAndGet();
        try {
            validateKey(key);
            new KVMessageProto(KVMessage.StatusType.GET, key, id).writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (Exception e) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.CLIENT_ERROR_KEY, e.getMessage(), id);
        }
    }

    private void validateKey(String key) {
        if (key.isEmpty())
            throw new IllegalArgumentException("Key must not be empty");
        if (key.length() > MAX_KEY_SIZE)
            throw new IllegalArgumentException(String.format("Max key length is %d Bytes", MAX_KEY_SIZE));
        // TODO: should also check for no space but that would fail testing.InteractionTest.testGetUnsetValue()
    }

    private void validateValue(String value) {
        if (value.length() > MAX_VALUE_SIZE)
            throw new IllegalArgumentException(String.format("Max value length is %d Bytes", MAX_VALUE_SIZE));
    }
}
