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
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class KVClient implements IKVClient {
    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient> ";
    private KVStore kvStore;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
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

    private void handleConnect(List<String> args) throws Exception {
        try {
            this.newConnection(args.get(0), Integer.parseInt(args.get(1)));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format("Unable to parse port from '%s'", args.get(1)));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(String.format("Unknown host '%s'", args.get(0)));
        } catch (Exception e) {
            throw new IOException(String.format("Could not establish connection to %s:%s", args.get(0), args.get(1)), e);
        }
    }

    private void handleDisconnect(List<String> args) {
        disconnect();
    }

    private void handlePut(List<String> args) throws Exception {
        if (kvStore == null) throw new IOException("Not connected to a server");
        KVMessage res = kvStore.put(args.get(0), args.size() == 1 ? null : args.get(1));
        System.out.println(res);
    }

    private void handleGet(List<String> args) throws Exception {
        if (kvStore == null) throw new IOException("Not connected to a server");
        KVMessage res = kvStore.get(args.get(0));
        System.out.println(res);
    }

    private void handleLogLevel(List<String> args) {
        String levelStr = LogSetup.UNKNOWN_LEVEL;
        Level level = Level.toLevel(args.get(0), null);
        if (level != null) {
            logger.setLevel(level);
            levelStr = level.toString();
        }

        if (levelStr.equals(LogSetup.UNKNOWN_LEVEL)) {
            throw new IllegalArgumentException(String.format("Invalid log level '%s'", args.get(0)));
        } else System.out.printf("Log level set to '%s'%n", levelStr);
    }

    private void handleHelp(List<String> args) {
        System.out.printf("Client for ECE419 Storage Server%n%s%n", Command.usage());
    }

    private void handleQuit(List<String> args) throws IOException {
        disconnect();
        System.in.close();
        System.out.println("Exiting...");
    }

    private void printError(String message, Throwable... errors) {
        if (errors.length == 0) {
            logger.error(message);
            System.out.printf("Error: %s%n", message);
        } else for (Throwable error : errors) {
            logger.error(message, error);
            System.out.printf("Error: %s: %s%n", message, error.getMessage());
        }
    }

    /**
     * Encapsulation of client commands as defined in Milestone 1's Quercus doc
     */
    private enum Command {
        connect("Tries to establish a TCP-connection to the storage server based on the given server address and the port number of the storage service.",
                2, 2, "host", "port"),
        disconnect("Tries to disconnect from the connected server.", 0, 0),
        put("Inserts a key-value pair into the storage server data structures.\n" +
                "Updates (overwrites) the current value with the given value if the server already contains the specified key.\n" +
                "Deletes the entry for the given key if value is omitted or is 'null'.",
                1, Integer.MAX_VALUE, "key", "value"),
        get("Retrieves the value for the given key from the storage server", 1, 1, "key"),
        logLevel(String.format("Sets the logger to the specified log level (one of %s)", LogSetup.getPossibleLogLevels()),
                1, 1, "level"),
        help("Print this message.", 0, 0),
        quit("Tears down the active connection to the server and exits the program.", 0, 0);

        public static final String ARG_DELIMITER = "\\s+";
        private final int minArgCount, maxArgCount;
        private final String helpText;
        private Callback callback = (str) -> logger.error(String.format("No callback registered for '%s'%n", this));

        Command(String description, int minArgCount, int maxArgCount, String... argNames) {
            this.minArgCount = minArgCount;
            this.maxArgCount = maxArgCount;
            this.helpText = String.format("%s %s\n%s",
                    this,
                    Arrays.stream(argNames).map(e -> String.format("<%s>", e)).collect(Collectors.joining(" ")),
                    indented(description)
            );
        }

        /**
         * @param commands varargs of commands to return usage text for; if empty, default to all commands
         * @return usage string for commands, including header
         */
        static String usage(Command... commands) {
            commands = commands.length == 0 ? values() : commands;
            return String.format("Usage:\n%s", Arrays.stream(commands).map(c -> indented(c.helpText)).collect(Collectors.joining("\n\n")));
        }

        /**
         * Safe wrapper of {@link Command#valueOf(String)} that returns null instead of throwing on error.
         * Also case-insensitive as a bonus.
         */
        static Command fromString(String name) {
            return Arrays.stream(values()).filter(e -> e.name().equalsIgnoreCase(name)).findAny().orElse(null);
        }

        /**
         * @param callback function that accepts a List<String> argument to be called by {@link Command#execute(String)}
         */
        void setCallback(Callback callback) {
            this.callback = callback;
        }

        /**
         * @param args commandline args to pass to the subcommand
         * @throws IllegalArgumentException if arguments are invalid
         * @throws Exception                propagated from callback
         */
        void execute(String args) throws Exception {
            long argCount = Arrays.stream(args.split(ARG_DELIMITER)).filter(Predicate.not(String::isEmpty)).count();
            List<String> tokens = Arrays.stream(args.split(ARG_DELIMITER, maxArgCount)).filter(Predicate.not(String::isEmpty)).collect(Collectors.toList());

            logger.info(String.format("'%s' called with %d args", this, argCount));
            if (argCount < minArgCount) {
                throw new IllegalArgumentException(String.format("Not enough args for '%s'", this));
            } else if (argCount > maxArgCount) {
                throw new IllegalArgumentException(String.format("Too many args for '%s'", this));
            }
            this.callback.accept(tokens);
        }

        /**
         * @param linesToIndent multi-line string to indent
         * @return string with each line prepended with a tab character
         */
        private static String indented(String linesToIndent) {
            return Arrays.stream(linesToIndent.split("\n")).map(e -> "\t" + e).collect(Collectors.joining("\n"));
        }

        /**
         * Functional interface that's basically a clone of a List<String> {@link java.util.function.Consumer},
         * except it can throw checked exceptions for propagation.
         */
        public interface Callback {
            void accept(List<String> args) throws Exception;
        }
    }

    /**
     * Wrapper to prompt the user for a command, looping until a non-blank string is given (or EOF/null, in which case we're done)
     */
    private static String readLine(BufferedReader reader) throws IOException {
        String line = "";
        while (line != null && line.isBlank()) {
            System.out.print(PROMPT);
            line = reader.readLine();
        }

        return line;
    }

    /**
     * Main entry point for the KVClient application.
     */
    public static void main(String[] args) {
        // 1. Initialize logger
        try {
            new LogSetup("logs/client.log", Level.ALL);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 2. Initialize client
        final KVClient kvClient = (KVClient) ObjectFactory.createKVClientObject();
        Command.connect.setCallback(kvClient::handleConnect);
        Command.disconnect.setCallback(kvClient::handleDisconnect);
        Command.put.setCallback(kvClient::handlePut);
        Command.get.setCallback(kvClient::handleGet);
        Command.logLevel.setCallback(kvClient::handleLogLevel);
        Command.help.setCallback(kvClient::handleHelp);
        Command.quit.setCallback(kvClient::handleQuit);

        // 3. Run client loop
        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
            for (String line; (line = readLine(stdin)) != null; ) {
                String[] tokens = line.split(Command.ARG_DELIMITER, 2);
                Command command = Command.fromString(tokens[0]);

                if (command == null) {
                    kvClient.printError(String.format("Unknown command '%s'", tokens[0]));
                    command = Command.help;
                }

                try {
                    command.execute(tokens.length == 1 ? "" : tokens[1]);
                } catch (Exception e) {
                    kvClient.printError(String.format("Command '%s' failed", command), e);
                    System.out.println(Command.usage(command));
                }
            }
        } catch (IOException e) {
            logger.info("CLI terminated", e);
        }
    }
}
