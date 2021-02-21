package app_kvECS;

import ecs.ECS;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ECSClient implements IECSClient {

    private static final String PROMPT = "ECSClient> ";
    private static final Logger logger = Logger.getRootLogger();
    private ECS ecs;

    public ECSClient(String config) {
        ecs = new ECS(config);
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public void handleAddNodes(List<String> args) {
        Collection<IECSNode> nodesAdded = addNodes(Integer.parseInt(args.get(0)), args.get(1), Integer.parseInt(args.get(2)));
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return ecs.addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
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
     * Encapsulation of client commands as defined in Milestone 1's Quercus doc
     */
    private enum Command {
        addNodes("Randomly chose number of nodes to queue for startup",
                3, 3, "numberOfNodes", "cacheStrategy", "cacheSize");

        public static final String ARG_DELIMITER = "\\s+";
        private final int minArgCount, maxArgCount;
        private final String helpText;
        private ECSClient.Command.Callback callback = (str) -> logger.error(String.format("No callback registered for '%s'%n", this));

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
        static String usage(ECSClient.Command... commands) {
            commands = commands.length == 0 ? values() : commands;
            return String.format("Usage:\n%s", Arrays.stream(commands).map(c -> indented(c.helpText)).collect(Collectors.joining("\n\n")));
        }

        /**
         * Safe wrapper of {@link ECSClient.Command#valueOf(String)} that returns null instead of throwing on error.
         * Also case-insensitive as a bonus.
         */
        static ECSClient.Command fromString(String name) {
            return Arrays.stream(values()).filter(e -> e.name().equalsIgnoreCase(name)).findAny().orElse(null);
        }

        /**
         * @param callback function that accepts a List<String> argument to be called by {@link ECSClient.Command#execute(String)}
         */
        void setCallback(ECSClient.Command.Callback callback) {
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

    private void printError(String message, Throwable... errors) {
        if (errors.length == 0) {
            logger.error(message);
            System.out.printf("Error: %s%n", message);
        } else for (Throwable error : errors) {
            logger.error(message, error);
            System.out.printf("Error: %s: %s%n", message, error.getMessage());
        }
    }

    public static void main(String[] args) {

        String path;
        // 1. Initialize logger
        try {
            new LogSetup("logs/client.log", Level.INFO);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 1. Validate args
        try {
            switch (args.length) {
                case 1:
                    path = args[0];
                    break;
                default:
                    throw new IllegalArgumentException("Invalid number of arguments");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e);
            System.err.println("Usage: ECS <ecsConfig>");
            System.exit(1);
            return;
        }

        // 2. Initialize ECS Server
        final ECSClient ecsClient = (ECSClient) ObjectFactory.createECSClientObject(path);
        Command.addNodes.setCallback(ecsClient::handleAddNodes);

        // 3. Run client loop
        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
            for (String line; (line = readLine(stdin)) != null; ) {
                String[] tokens = line.split(ECSClient.Command.ARG_DELIMITER, 2);
                ECSClient.Command command = ECSClient.Command.fromString(tokens[0]);

                if (command == null) {
                    ecsClient.printError(String.format("Unknown command '%s'", tokens[0]));
                }

                try {
                    command.execute(tokens.length == 1 ? "" : tokens[1]);
                } catch (Exception e) {
                    ecsClient.printError(String.format("Command '%s' failed", command), e);
                    System.out.println(ECSClient.Command.usage(command));
                }
            }
        } catch (IOException e) {
            logger.info("CLI terminated", e);
        }
    }
}
