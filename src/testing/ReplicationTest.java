package testing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import ecs.ECS;
import ecs.IECSNode;
import junit.framework.TestCase;
import logger.LogSetup;
import shared.MD5;

public class ReplicationTest extends TestCase {
    private ECSClient ecsClient;
    private static final Logger logger = Logger.getLogger(ReplicationTest.class);

    @Before
    public void setUp() {
        ecsClient = AllTests.getECS();
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
}
