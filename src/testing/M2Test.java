package testing;

import java.net.UnknownHostException;
import java.util.*;

import app_kvServer.KVServer;
import ecs.ECS;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.apache.log4j.Logger; // import Logger

import app_kvECS.ECSClient;

import junit.framework.Test;
import junit.framework.TestSuite;

/* 
    Integration of the JUnit library into your project (as in Milestone 1)
    Automate the running of the given test cases Do regression testing, 
    make sure that relevant Milestone 1 tests still pass Test functionality such as create connection/disconnect,get/put value, update value (existing key), get non-existing key(check error messages)

    Add at least 10 test cases of your choice that cover the additional functionality and features of this milestone (e.g., ECS,consistent hashing, metadata updates, retry operations, locks ,etc.)

    Compile a short test report about all test cases, especially your own cases (submit as appendix of your design document).
*/
public class M2Test extends TestCase {
    private ECSClient ecsClient;
    private static final Logger logger = Logger.getLogger(M2Test.class);

    protected void setUp() {
        ecsClient = new ECSClient("127.0.0.1", 50000);
        ecsClient.start();
    }

    protected void tearDown() {
        if (ecsClient != null) 
            ecsClient.shutdown();
    }

    public void testECSClientInitialization() {
        assertNotNull(ecsClient);
        assertTrue(ecsClient.clientRunning);
    }

    public void testStartECSService() {
        assertTrue(ecsClient.ecsRunning);
    }

    public void testStopECSService() {
        ecsClient.stop();
        assertFalse(ecsClient.ecsRunning);
    }

    public void testShutdownECS() {
        ecsClient.shutdown();
        assertFalse(ecsClient.ecsRunning);
    }

    public void testAddNode() {
        IECSNode node = ecsClient.addNode("LRU", 1024);
        assertNotNull(node);
    }

    public void testRemoveNodes() {
        Set<String> nodeNames = new HashSet<>();
        IECSNode node = ecsClient.addNode("LRU", 1024);
        assertNotNull(node);
        nodeNames.add(node.getNodeName());
        boolean removed = ecsClient.removeNodes(nodeNames);
        assertTrue(removed);
    }

    public void testConsistentHashing() {
    }

    public void testMetadataUpdates() {
    }

    public void testRetryOperations() {
    }

    public void testLocks() {

    }

}