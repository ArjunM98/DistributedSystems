package app_kvServer;

import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer extends Thread implements IKVServer {

    private int port;
    private long cacheSize;
    private String strategy;
    private ServerSocket serverSocket;
    private boolean running;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        // TODO - finish implementation (mock KVServer basics)
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
    }

    @Override
    public int getPort() {
        // TODO Auto-generated method stub
        return -1;
    }

    @Override
    public String getHostname() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        // TODO Auto-generated method stub
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public int getCacheSize() {
        // TODO Auto-generated method stub
        return -1;
    }

    @Override
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
    }

    @Override
    public void run() {
        // TODO Mock implementation of run() for tester

        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client, this /* reference to server process */);
                    new Thread(connection).start();
                } catch (IOException e) {
                    System.out.println("Error!");
                }
            }
        }
    }

    @Override
    public void kill() {
        // TODO Auto-generated method stub
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

    private boolean initializeServer() {
        try {
            serverSocket = new ServerSocket(port);
            return true;
        } catch (IOException e) {
            if (e instanceof BindException) {
                // TODO replace with logger
                System.out.println("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    private boolean isRunning() {
        return running;
    }
}
