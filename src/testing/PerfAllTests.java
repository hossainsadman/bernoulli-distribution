package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class PerfAllTests {

    static {
        try {
            new LogSetup("logs/testing/perftest.log", Level.WARN);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Storage Server Performance Test Suite");
        // clientSuite.addTestSuite(PerfNoCacheTest.class); 
        // clientSuite.addTestSuite(PerfLRUCacheTest.class); 
        // clientSuite.addTestSuite(PerfLFUCacheTest.class); 
        clientSuite.addTestSuite(PerfFIFOCacheTest.class); 
        return clientSuite;
    }
    
}