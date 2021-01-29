package testing;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;


public class AllTests {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            KVServer server = new KVServer(50000, 10, "FIFO");
            server.clearStorage(); // idk if we're allowed to do this
            server.start(); // or this, but we need it to run ¯\_(ツ)_/¯
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        clientSuite.addTestSuite(AdditionalTest.class);
        return clientSuite;
    }

}
