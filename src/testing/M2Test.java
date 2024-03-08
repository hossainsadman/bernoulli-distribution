package testing;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import app_kvECS.ECSClient;
import ecs.IECSNode;
import junit.framework.TestCase;

public class M2Test extends TestCase{
    private ECSClient ecsClient;
    private static final Logger logger = Logger.getLogger(M2Test.class);

    @Before
    public void setUp() {
        ecsClient = new ECSClient("127.0.0.1", 20000);
        ecsClient.start();
    }

    @After
    public void tearDown() {
        if (ecsClient != null) {
            ecsClient.shutdown();
        }
    }

    @Test
    public void testECSClientInitialization() {
        assertNotNull("ECSClient should not be null", ecsClient);
        assertTrue("ECSClient should be running", ecsClient.clientRunning);
    }

    @Test
    public void testStartECSService() {
        assertTrue("ECSService should be running", ecsClient.ecsRunning);
    }

    @Test
    public void testStopECSService() {
        ecsClient.stop();
        assertFalse("ECSService should not be running", ecsClient.ecsRunning);
    }

    public void testShutdownECS() {
        ecsClient.shutdown();
        assertFalse(ecsClient.ecsRunning);
    }
}
