package testing;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import testing.performance.Server01Client01PerformanceTest;
import testing.performance.Server01Client05PerformanceTest;
import testing.performance.Server01Client20PerformanceTest;

import java.io.IOException;

public class PerformanceTests extends TestCase {
    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage Server PERFORMANCE Test-Suite");
        clientSuite.addTestSuite(Server01Client01PerformanceTest.class);
        clientSuite.addTestSuite(Server01Client05PerformanceTest.class);
        clientSuite.addTestSuite(Server01Client20PerformanceTest.class);
        return clientSuite;
    }
}
