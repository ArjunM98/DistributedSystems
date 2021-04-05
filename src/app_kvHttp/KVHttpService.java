package app_kvHttp;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class KVHttpService {
    private static final Logger logger = Logger.getRootLogger();

    /**
     * Main entry point for the KVHttpService application.
     *
     * @param args contains [portNumber, zkConn [, logLevel]]
     */
    public static void main(String[] args) {
        // 0. Default args
        int portNumber;
        String connectionString;
        Level logLevel = Level.ALL;

        // 1. Validate args
        try {
            switch (args.length) {
                case 3:
                    String candidateLevel = args[2].toUpperCase();
                    if (!LogSetup.isValidLevel(candidateLevel))
                        throw new IllegalArgumentException(String.format("Invalid log level '%s'", candidateLevel));
                    logLevel = Level.toLevel(candidateLevel, logLevel);
                case 2:
                    connectionString = args[1];
                    try {
                        portNumber = Integer.parseInt(args[0]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("Invalid port number '%s'", args[0]));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Invalid number of arguments");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e);
            System.err.println("Usage: Http <port> <connectionString> [<loglevel>]");
            System.exit(1);
            return;
        }

        // 2. Initialize logger
        try {
            new LogSetup("logs/http.log", logLevel);
        } catch (IOException e) {
            System.err.println("Logger error: " + e);
            System.exit(1);
            return;
        }

        // TODO: 3. Run server and respond to ctrl-c and kill
        logger.error("HTTP Server stub not implemented. Would have started with:");
        logger.error("portNumber = " + portNumber);
        logger.error("connectionString = " + connectionString);
        logger.error("logLevel = " + logLevel);
    }
}
