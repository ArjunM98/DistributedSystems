package app_kvClient;

import client.KVCommInterface;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.ObjectFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KVClient implements IKVClient {
    private static final Logger logger = Logger.getRootLogger();

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        // TODO Auto-generated method stub
        logger.warn("KVClient.newConnection(String, int) not implemented");
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        logger.warn("KVClient.getStore() not implemented");
        return null;
    }

    /**
     * Main entry point for the KVClient application.
     *
     * @param args contains optional [logLevel]
     */
    public static void main(String[] args) {
        Level logLevel = Level.ALL;

        // 1. Validate args
        try {
            switch (args.length) {
                case 1:
                    String candidateLevel = args[0].toUpperCase();
                    if (!LogSetup.isValidLevel(candidateLevel))
                        throw new IllegalArgumentException(String.format("Invalid log level '%s'", candidateLevel));
                    logLevel = Level.toLevel(candidateLevel, Level.ALL);
                case 0:
                    break;
                default:
                    throw new IllegalArgumentException("Invalid number of arguments");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e);
            System.err.println("Usage: Client [<loglevel>]");
            System.exit(1);
            return;
        }

        // 2. Initialize logger
        try {
            new LogSetup("logs/client.log", logLevel);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // 3. Run client
        final IKVClient client = ObjectFactory.createKVClientObject();

        System.out.println("TODO: Implement Me (ctrl-d to exit)");
        System.out.print("> ");
        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = stdin.readLine()) != null) {
                logger.debug(String.format("Received: '%s'", line));
                System.out.print("> ");
            }
        } catch (Exception e) {
            logger.error(String.format("Exception caught: '%s'", e));
        }
    }
}
