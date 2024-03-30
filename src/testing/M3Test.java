package testing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import app_kvECS.ECSClient;
import client.KVStore;
import ecs.ECSHashRing;
import ecs.ECSNode;
import logger.LogSetup;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;

public class M3Test {
    private static ECSClient ecsClient;
    private static ECSNode firstServer, secondServer, thirdServer, fourthServer, fifthServer;
    private static ArrayList<ECSNode> allNodes;
    private static final Logger logger = Logger.getLogger(M3Test.class);
    public static KVStore kvClient;

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            ecsClient = new ECSClient("127.0.0.1", 20000);
            ecsClient.setTesting(true);
            ecsClient.start();

            allNodes = new ArrayList<>();
            firstServer = ecsClient.addNode("FIFO", 10, 5004);
            secondServer = ecsClient.addNode("FIFO", 10, 5002);
            thirdServer = ecsClient.addNode("FIFO", 10, 5003);
            fourthServer = ecsClient.addNode("FIFO", 10, 5001);
            // fifthServer = ecsClient.addNode("FIFO", 10, 5005);
            allNodes.add(firstServer);
            allNodes.add(secondServer);
            allNodes.add(thirdServer);
            allNodes.add(fourthServer);
            // allNodes.add(fifthServer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
	public static void tearDown() {
        for (ECSNode node : allNodes) {
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    private static void createNode(int port){
        ECSNode node = ecsClient.addNode("FIFO", 10, port);
        allNodes.add(node);
    }

    private static KVStore createKVClient(String host, int port) {
        KVStore kvClient = new KVStore(host, port);
        kvClient.setTesting(true);
        kvClient.setMaxRetries(0);
        return kvClient;
    }

    private static ECSNode getReponsibleNode(String key) {
        ECSHashRing hashRing = ecsClient.getECS().getHashRing();
        return hashRing.getNodeForKey(key);
    }

    private static ECSNode[] getReplicas(ECSNode node) {
        ECSHashRing hashRing = ecsClient.getECS().getHashRing();
        return hashRing.getNextTwoNodeSuccessors(node);
    }

    private static ECSNode[] getNotReplicas(ECSNode node) {
        ECSHashRing hashRing = ecsClient.getECS().getHashRing();
        ECSNode[] replicas = hashRing.getNextTwoNodeSuccessors(node);
        ArrayList<ECSNode> notReplicasList = new ArrayList<>();

        for (ECSNode _node : allNodes) { 
            if (_node.getNodeIdentifier() != node.getNodeIdentifier() && _node.getNodeIdentifier() != replicas[0].getNodeIdentifier() && _node.getNodeIdentifier() != replicas[1].getNodeIdentifier()) {
                notReplicasList.add(_node);
            } 
        }

        return notReplicasList.toArray(new ECSNode[notReplicasList.size()]);
    }

    private static void removeNodeFromAllNodes(ECSNode node) {
        Iterator<ECSNode> iterator = allNodes.iterator();
        while (iterator.hasNext()) {
            ECSNode _node = iterator.next();
            if (_node.getNodeIdentifier().equals(node.getNodeIdentifier())) {
                iterator.remove();
            }
        }
    }    

    @Test
    public void testGetKeyFromOneReplicaServer() {
        String key = "aba";
        String value = "bar";
        BasicKVMessage response = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);

            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response = kvClient.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }
        System.out.println(response.getStatus() + " " + response.getValue());
        assertTrue(ex == null && response.getValue().equals("bar") 
                && response.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    public void testGetKeyFromTwoReplicaServer() {
        String key = "foo1";
        String value = "bar";
        BasicKVMessage response1 = null, response2 = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
        
            // first replica
            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response1 = kvClient.get(key);
            
            // second replica
            kvClient.reconnect(replicas[1].getNodeHost(), replicas[1].getNodePort());
            response2 = kvClient.get(key);

        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertTrue(ex == null && response1.getValue().equals("bar") 
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response2.getValue().equals("bar")
                && response1.getStatus() == StatusType.GET_SUCCESS);
        
    }

    @Test
    public void testOnlyReplicatedToTwoServers() {
        String key = "foo2";
        String value = "bar";
        BasicKVMessage response1 = null, response2 = null, response3 = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);
        ECSNode[] notReplicas = getNotReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
            
            // first replica
            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response1 = kvClient.get(key);
            
            // second replica
            kvClient.reconnect(replicas[1].getNodeHost(), replicas[1].getNodePort());
            response2 = kvClient.get(key);
            
            // third replica
            kvClient.reconnect(notReplicas[0].getNodeHost(), notReplicas[0].getNodePort());
            response3 = kvClient.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        System.out.println(response1.getStatus() + " " + response2.getStatus() + " " + response3.getStatus());
        System.out.println(response1.getValue() + " " + response2.getValue() + " " + response3.getValue());
        assertTrue(ex == null && response1.getValue().equals("bar") 
                && response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response2.getValue().equals("bar")
                && response3.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
    }

    @Test
    public void testPutUpdateOnReplicas(){
        String key = "foo3";
        String value = "will update value";
        String updatedValue = "updated value";
        BasicKVMessage response = null, response1 = null, response2 = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, updatedValue);
        
            // first replica
            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response1 = kvClient.get(key);
            
            // second replica
            kvClient.reconnect(replicas[1].getNodeHost(), replicas[1].getNodePort());
            response2 = kvClient.get(key);

        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue)
                && response1.getValue().equals(updatedValue)
                && response2.getValue().equals(updatedValue)
                && response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    public void testDeleteOnReplicas(){
        String key = "abc123";
        String value = "about to delete";
        BasicKVMessage response = null, response1 = null, response2 = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, "null");
        
            // first replica
            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response1 = kvClient.get(key);
            
            // second replica
            kvClient.reconnect(replicas[1].getNodeHost(), replicas[1].getNodePort());
            response2 = kvClient.get(key);

        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS
                && response1.getStatus() == StatusType.GET_ERROR
                && response2.getStatus() == StatusType.GET_ERROR);
    }

    @Test
    public void testPutUpdateAfterCoordinatorFails() {
        String key = "abc123";
        String value = "about to update";
        BasicKVMessage response = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            removeNodeFromAllNodes(node);

            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            response = kvClient.put(key, "updated");

        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertTrue(ex == null && response != null &&response.getStatus() == StatusType.PUT_UPDATE);
    }

    @Test
    public void testNewReplicaPropogationAfterCoordinatorFails() {
        createNode(5006);
        String key = "tetsing";
        String value = "test";
        BasicKVMessage response1 = null, response2 = null, response3 = null;
        ECSNode node = getReponsibleNode(key);
        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            kvClient.put(key, value);
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            removeNodeFromAllNodes(node);
            ECSNode newNodeResponsible = getReponsibleNode(key);
            ECSNode[] newReplicas = getReplicas(newNodeResponsible);
            kvClient.reconnect(newNodeResponsible.getNodeHost(), newNodeResponsible.getNodePort());
            response1 = kvClient.get(key);
            kvClient.reconnect(newReplicas[0].getNodeHost(), newReplicas[0].getNodePort());
            response2 = kvClient.get(key);
            kvClient.reconnect(newReplicas[1].getNodeHost(), newReplicas[1].getNodePort());
            response3 = kvClient.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(response1.getStatus() == StatusType.GET_SUCCESS
                && response2.getStatus() == StatusType.GET_SUCCESS
                && response3.getStatus() == StatusType.GET_SUCCESS);
    }

    @Test
    public void testShutdownRestore() {
        createNode(5007);
        String key = "onetwothre";
        String value = "test";
        BasicKVMessage response = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(key);
        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
            kvClient.put(key, value);
            TimeUnit.MILLISECONDS.sleep(500);
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            removeNodeFromAllNodes(node);
            TimeUnit.MILLISECONDS.sleep(500);
            createNode(node.getNodePort());
            TimeUnit.MILLISECONDS.sleep(500);

            kvClient.reconnect(node.getNodeHost(), node.getNodePort());
            response = kvClient.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        if (response != null){
            System.out.println("rEPONSE IS NOT NULL");
            System.out.println(response.getStatus());
            System.out.println(response.getKey());
            System.out.println(response.getValue());    
        } else {
            System.out.println("rEPONSE IS NULL");
        }

        assertTrue(ex == null && response != null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue().equals(value));
    }

}

/**
 * e73eb7edc6b16f4bfdbfe7bd78f9ac14,1898ed79a9d5451a3af7c309d5a43f0a,127.0.0.1:5007;
 * 1898ed79a9d5451a3af7c309d5a43f0a,866b8c19657a7bce49f19d8a752e060c,127.0.0.1:5004;
 * 866b8c19657a7bce49f19d8a752e060c,a55ce0afec21e9932739a9d707df9b3c,127.0.0.1:5002;
 * a55ce0afec21e9932739a9d707df9b3c,b07a61972b0a0e3c8ad559e0037dfcd2,127.0.0.1:5003;
 * b07a61972b0a0e3c8ad559e0037dfcd2,e73eb7edc6b16f4bfdbfe7bd78f9ac14,127.0.0.1:5001;
 * 
 * 
 * e73eb7edc6b16f4bfdbfe7bd78f9ac14,1898ed79a9d5451a3af7c309d5a43f0a,127.0.0.1:5007;
 * 1898ed79a9d5451a3af7c309d5a43f0a,866b8c19657a7bce49f19d8a752e060c,127.0.0.1:5004;
 * 866b8c19657a7bce49f19d8a752e060c,a55ce0afec21e9932739a9d707df9b3c,127.0.0.1:5002;
 * a55ce0afec21e9932739a9d707df9b3c,b07a61972b0a0e3c8ad559e0037dfcd2,127.0.0.1:5003;
 * b07a61972b0a0e3c8ad559e0037dfcd2,e73eb7edc6b16f4bfdbfe7bd78f9ac14,127.0.0.1:5001;
 */

