package client;

import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVStore implements KVCommInterface {

    private final String addr;
    private final int portNum;

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    private long msgID;
    private boolean running;

    private static final int MAX_KEY_SIZE = 20;
    private static final int MAX_VALUE_SIZE = 120 * 1024;

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        addr = address;
        portNum = port;
    }

    @Override
    public void connect() throws Exception {
        clientSocket = new Socket(addr, portNum);
        input = clientSocket.getInputStream();
        output = clientSocket.getOutputStream();
        msgID = 0;
        setRunning(true);
    }

    @Override
    public void disconnect() {
        try {
            tearDownConnection();
        } catch (IOException e) {
            // TODO: replace with logger
            System.out.println(e);
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {

        incrementMsgID();

        if (malformedKey(key) & malformedValue(value)) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, key, value, msgID);
        }

        try {
            KVMessageProto req = new KVMessageProto(KVMessage.StatusType.PUT, key, value, msgID);
            req.writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (IOException e) {
            if (isRunning()) {
                disconnect();
            }
            throw e;
        }
    }

    @Override
    public KVMessage get(String key) throws Exception {

        incrementMsgID();

        if (malformedKey(key)) {
            return new KVMessageProto(KVMessage.StatusType.FAILED, key, "" /* empty value */, msgID);
        }

        try {
            KVMessageProto req = new KVMessageProto(KVMessage.StatusType.GET, key, "" /* empty value */, msgID);
            req.writeMessageTo(output);
            return new KVMessageProto(input);
        } catch (IOException e) {
            if (isRunning()) {
                disconnect();
            }
            throw e;
        }
    }

    private void setRunning(boolean run) {
        running = run;
    }

    private boolean isRunning() {
        return running;
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
        }
    }

    private boolean malformedKey(String key) {
        if (key.length() == 0) {
            return true;
        } else return key.length() >= MAX_KEY_SIZE;
    }

    private boolean malformedValue(String value) {
        return value.length() >= MAX_VALUE_SIZE;
    }

    private void incrementMsgID() {
        msgID += 1;
    }

}
