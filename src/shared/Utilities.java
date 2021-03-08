package shared;

import org.apache.log4j.Logger;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utilities {
    private static final Logger logger = Logger.getRootLogger();

    public static String getHostname() {
        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getCanonicalHostName();
        } catch (Exception e) {
            logger.error("Unable to get public IP address", e);
        }

        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException ex) {
            logger.error("Unknown host: try starting the server first", ex);
        }
        return "0.0.0.0";
    }
}
