package testing;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

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
    public void testECSClientInitialization() {
        ECSClient ecsClient = new ECSClient("127.0.0.1", 50000);
        assertNotNull(ecsClient);
        assertTrue(ecsClient.clientRunning);
    }

    public void testStartECSService() {
        ECSClient ecsClient = new ECSClient("127.0.0.1", 30000);
        boolean started = ecsClient.start();
        assertTrue(started);
        assertTrue(ecsClient.ecsRunning);
    }

    public void testStopECSService() {
        ECSClient ecsClient = new ECSClient("127.0.0.1", 30001);
        ecsClient.start();
        ecsClient.stop();
        assertFalse(ecsClient.ecsRunning);
    }

    public void testShutdownECS() {
        ECSClient ecsClient = new ECSClient("127.0.0.1", 30002);
        ecsClient.start();
        boolean shutdown = ecsClient.shutdown();
        assertTrue(shutdown);
        assertFalse(ecsClient.ecsRunning);
    }

    public void testAddNode() {
        ECSClient ecsClient = new ECSClient("127.0.0.1", 50000);
        ecsClient.start();
        IECSNode node = ecsClient.addNode("LRU", 1024);
        assertNotNull(node);
    }

    public void testRemoveNodes() {
        return;
    }
}
