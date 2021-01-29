package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
    }

    @Override
    public void disconnect() {
        if (input == null && output == null && clientSocket == null) return;
        try {
            List<String> errors = new ArrayList<>();

            try {
                Objects.requireNonNull(input).close();
            } catch (Exception e) {
                errors.add(String.format("InputStream (%s)", e.getMessage()));
            }
            try {
                Objects.requireNonNull(output).close();
            } catch (Exception e) {
                errors.add(String.format("OutputStream (%s)", e.getMessage()));
            }
            try {
                Objects.requireNonNull(clientSocket).close();
            } catch (Exception e) {
                errors.add(String.format("Socket (%s)", e.getMessage()));
            }
            input = null;
            output = null;
            clientSocket = null;
            if (!errors.isEmpty()) throw new IOException(String.join(", ", errors));
        } catch (IOException e) {
            logger.error("Error disconnecting from session", e);
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        final long id = msgID.incrementAndGet();
        value = value == null ? "null" : value;

        if (malformedKey(key)) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.ERROR_KEY, "Malformed Key", id);
        } else if (malformedValue(value)) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.ERROR_KEY, "Malformed Value", id);
        }

        try {
            KVMessageProto req = new KVMessageProto(KVMessage.StatusType.PUT, key, value, id);
            req.writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (IOException e) {
            disconnect();
            throw e;
        }
    }

    @Override
    public KVMessage get(String key) throws Exception {
        final long id = msgID.incrementAndGet();

        if (malformedKey(key)) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, KVMessageProto.ERROR_KEY, "Malformed Key", id);
        }

        try {
            KVMessageProto req = new KVMessageProto(KVMessage.StatusType.GET, key, "" /* empty value */, id);
            req.writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (IOException e) {
            disconnect();
            throw e;
        }
    }

    private boolean malformedKey(String key) {
        // TODO: should also check for no space but that would fail testing.InteractionTest.testGetUnsetValue()
        return key.isEmpty() || key.length() >= MAX_KEY_SIZE;
    }

    private boolean malformedValue(String value) {
        return value.length() >= MAX_VALUE_SIZE;
    }
}
