package testing;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import shared.ObjectFactory;

import java.io.IOException;


public class AllTests {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            ObjectFactory.createKVServerObject(50000, 10, "FIFO");
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
