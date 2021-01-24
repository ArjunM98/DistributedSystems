package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import org.apache.log4j.Level;
import shared.messages.KVMessage;
import shared.messages.KVMessageProto;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

    private KVStore commInt;
    private boolean running = true;
    private BufferedReader stdin;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        commInt = new KVStore(hostname, port);
        commInt.connect();
    }

    @Override
    public KVCommInterface getStore(){
        return commInt;
    }

    private void disconnect() {
        if(commInt != null){
            commInt.disconnect();
            commInt = null;
        }
    }

    private void handleCommand(String cmdLine) {

        // There will be no space in a key or value pair
        String[] tokens = cmdLine.split("\\s");

        if(tokens[0].equals("quit")) {
            running = false;
            disconnect();
        } else if (tokens[0].equals("connect") && validParams(tokens, 3)){
            try{
                newConnection(tokens[1], Integer.parseInt(tokens[2]));
            } catch(NumberFormatException nfe) {
                // TODO add loggers
                printError("No valid address. Port must be a number!");
            } catch (UnknownHostException e) {
                // TODO add loggers
                printError("Unknown Host!");
            } catch (IOException e) {
                // TODO add loggers
                printError("Could not establish connection!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (tokens[0].equals("disconnect")) {
            disconnect();
        } else if (tokens[0].equals("put") || tokens[0].equals("get")) {
            sendMessage(tokens);
        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void sendMessage(String[] tokens) {
        if (tokens[0].equals("put") && validParams(tokens, 3 /* num params */)) {
            try {
                KVMessage res = commInt.put(tokens[1], tokens[2]);
            } catch (Exception e) {
                // TODO: replace with logger
                disconnect();
                e.printStackTrace();
            }
        } else if (tokens[0].equals("get") && validParams(tokens, 2 /* num params */)) {
            try {
                KVMessage res = commInt.get(tokens[1]);
            } catch (Exception e) {
                // TODO: replace with logger
                disconnect();
                e.printStackTrace();
            }
        }
    }

    private boolean validParams(String[] tokens, int numParams) {
        if (tokens.length == 3) {
            return true;
        }
        printError("Invalid number of parameters!");
        return false;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("CLIENT HELP (Usage):\n");
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");

        sb.append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server \n");

        sb.append("put <key> <value>");
        sb.append("\t\t\t Inserts a key-value pair into the storage server data structures \n");
        sb.append("\t\t\t\t Updates (overwrites) the current value with the given value if the server already contains the specified key \n");
        sb.append("\t\t\t\t\t Deletes the entry for the given key if <value> equals null \n");

        sb.append("disconnect");
        sb.append("\t\t\t\t\t\t disconnects from the server \n");

        sb.append("quit");
        sb.append("\t\t\t\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printError(String error){
        System.out.println("Error! " +  error);
    }

    public void run() {
        while(running) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        // TODO need to add logger
        KVClient app = new KVClient();
        app.run();
    }

}
