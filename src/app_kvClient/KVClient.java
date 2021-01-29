package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Arrays;


public class KVClient implements IKVClient {

    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private KVStore kvStore;
    private boolean running = true;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        logger.info(String.format("New Connection established at: %s:%d", hostname, port));
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    private void disconnect() {
        if (kvStore != null) {
            kvStore.disconnect();
            kvStore = null;
        }
    }

    private void handleCommand(String cmdLine) {

        // There will be no space in a key & value pair
        String[] tokens = cmdLine.split("\\s", 3);

        switch (tokens[0]) {
            case "quit":
                running = false;
                disconnect();
                System.out.println(PROMPT + "Application exit!");
                break;
            case "connect":
                if (validParams(tokens, 3)) {
                    try {
                        newConnection(tokens[1], Integer.parseInt(tokens[2]));
                    } catch (NumberFormatException nfe) {
                        logger.info("Unable to parse argument <port>", nfe);
                        printError("No valid address. Port must be a number!");
                    } catch (UnknownHostException e) {
                        logger.info("Unknown Host!", e);
                        printError("Unknown Host!");
                    } catch (IOException e) {
                        logger.warn("Could not establish connection!", e);
                        printError("Could not establish connection!");
                    } catch (Exception e) {
                        logger.warn("Unknown exception!", e);
                    }
                }
                break;
            case "disconnect":
                disconnect();
                break;
            case "put":
            case "get":
                sendMessage(tokens);
                break;
            case "logLevel":
                if (validParams(tokens, 2 /* num params */)) {
                    String level = setLogLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT + "Log level changed to level " + level);
                    }
                }
                break;
            case "help":
                printHelp();
                break;
            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }

    private void sendMessage(String[] tokens) {
        if (tokens[0].equals("put") && validParams(tokens, 2, 3)) {
            try {
                KVMessage res = kvStore.put(tokens[1], tokens.length == 2 ? "null" : tokens[2]);
                System.out.printf("%s%s%n", PROMPT, res);
            } catch (Exception e) {
                System.out.println(PROMPT + "Server not connected");
                logger.info("Server disconnected: " + e.getMessage());
                logger.error(String.format("Unable to execute put request, exception caught: '%s'", e));
                disconnect();
            }
        } else if (tokens[0].equals("get") && validParams(tokens, 2 /* num params */)) {
            try {
                KVMessage res = kvStore.get(tokens[1]);
                System.out.printf("%s%s%n", PROMPT, res);
            } catch (Exception e) {
                System.out.println(PROMPT + "Server not connected");
                logger.info("Server disconnected: " + e.getMessage());
                logger.error(String.format("Unable to execute get request, exception caught: '%s'", e));
                disconnect();
            }
        }
    }

    private boolean validParams(String[] tokens, int... numParams) {
        if (Arrays.stream(numParams).anyMatch(e -> e == tokens.length)) return true;
        printError("Invalid number of parameters!");
        return false;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t Retrieves the value for the given key from the storage server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t Inserts a key-value pair into the storage server data structures \n");
        sb.append("\t\t\t\t Updates (overwrites) the current value with the given value if the server already contains the specified key \n");
        sb.append("\t\t\t\t Deletes the entry for the given key if <value> equals null \n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t disconnects from the server \n");

        sb.append(PROMPT).append("quit");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + LogSetup.getPossibleLogLevels());
    }

    private String setLogLevel(String levelString) {
        Level level = Level.toLevel(levelString, null);
        if (level != null) {
            logger.setLevel(level);
            return level.toString();
        } else return LogSetup.UNKNOWN_LEVEL;
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public void run() {
        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
            while (running) {
                System.out.print(PROMPT);
                this.handleCommand(stdin.readLine());
            }
        } catch (IOException e) {
            running = false;
            logger.info("Unresponsive CLI", e);
            printError("CLI does not respond - Application terminated ");
        }
    }

    /**
     * Main entry point for the KVClient application.
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            final KVClient kvClient = (KVClient) ObjectFactory.createKVClientObject();
            kvClient.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            logger.error("Could not initialize logger", e);
            System.exit(1);
        }
    }
}
