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
import app_kvServer.SQLTable;
import client.KVStore;
import ecs.ECSHashRing;
import ecs.ECSNode;
import logger.LogSetup;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;

public class M4Test {
    private static ECSClient ecsClient;
    private static ECSNode firstServer, secondServer, thirdServer, fourthServer, fifthServer;
    private static ArrayList<ECSNode> allNodes;
    private static final Logger logger = Logger.getLogger(M4Test.class);
    public static KVStore kvClient;

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
            ecsClient = new ECSClient("127.0.0.1", 20000);
            ecsClient.setTesting(true);
            ecsClient.start();

            allNodes = new ArrayList<>();
            firstServer = ecsClient.addNode("FIFO", 10, 5004);
            secondServer = ecsClient.addNode("FIFO", 10, 5002);
            thirdServer = ecsClient.addNode("FIFO", 10, 5003);
            fourthServer = ecsClient.addNode("FIFO", 10, 5001);
            fifthServer = ecsClient.addNode("FIFO", 10, 5005);
            allNodes.add(firstServer);
            allNodes.add(secondServer);
            allNodes.add(thirdServer);
            allNodes.add(fourthServer);
            allNodes.add(fifthServer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
	public static void tearDown() {
        for (ECSNode node : allNodes) {
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            try {
                TimeUnit.MILLISECONDS.sleep(100);
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
    public void testSQLCreateInvalid() {
        String tableName = "testSQLCreateInvalid";
        String schema = "age:INVALIDTYPE,student:text";
        BasicKVMessage resCreate = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resCreate = kvClient.sqlcreate(tableName, schema);
            resSelect = kvClient.sqlselect(tableName);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resCreate != null) {
            this.logger.info("resCreate " + resCreate.getStatus());
        } else {
            this.logger.info("resCreate is null");
        }

        if (resSelect != null) {
            this.logger.info("resSelect " + resSelect.getStatus());
        } else {
            this.logger.info("resSelect is null");
        }

        assertTrue(resCreate.getStatus() == StatusType.SQLCREATE_ERROR
                && resSelect.getStatus() == StatusType.SQLSELECT_ERROR);
    }

    @Test 
    public void testSQLCreateValid() {
        this.logger.info("--- testSQLCreateValid() ---");
        String tableName = "testSQLCreateValid";
        String schema = "age:int,student:text";
        BasicKVMessage resCreate = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resCreate = kvClient.sqlcreate(tableName, schema);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resCreate != null) {
            this.logger.info("resCreate " + resCreate.getStatus());
        } else {
            this.logger.info("resCreate is null");
        }

        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
        } else {
            this.logger.info("resSelect is null");
        }

        SQLTable table = SQLTable.fromString(resSelect.getValue());
        System.out.println(table.toStringTable());

        assertTrue(ex == null) ;
        assertTrue(resCreate.getStatus() == StatusType.SQLCREATE_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 0);
        assertTrue(table.getCols().size() == 2);
        assertTrue(table.getCols().contains("age"));
        assertTrue(table.getCols().contains("student"));
        assertTrue(table.getColTypes().size() == 2);
        assertTrue(table.getColTypes().containsValue("int"));
        assertTrue(table.getColTypes().containsValue("text"));
    }

    @Test
    public void testSQLInsertInvalid() {
        this.logger.info("--- testSQLInsertInvalid() ---");
        String tableName = "testSQLCreateValid";
        String invalidRow = "age\":20,\"student\":ValidStudent}"; // missing {\" in front
        BasicKVMessage resInsert = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resInsert = kvClient.sqlinsert(tableName, invalidRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resInsert != null) {
            this.logger.info("resInsert " + resInsert.getStatus());
        } else {
            this.logger.info("resInsert is null");
        }

        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
        } else {
            this.logger.info("resSelect is null");
        }

        SQLTable table = SQLTable.fromString(resSelect.getValue());
        System.out.println(table.toStringTable());

        assertTrue(ex == null) ;
        assertTrue(resInsert.getStatus() == StatusType.SQLINSERT_ERROR);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 0);
    }

    @Test
    public void testSQLInsertValid() {
        this.logger.info("--- testSQLInsertValid() ---");
        String tableName = "testSQLCreateValid";
        String validRow = "{\"age\":10,\"student\":ValidStudent}";
        BasicKVMessage resInsert = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resInsert = kvClient.sqlinsert(tableName, validRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resInsert != null) {
            this.logger.info("resInsert " + resInsert.getStatus());
        } else {
            this.logger.info("resInsert is null");
        }

        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
        } else {
            this.logger.info("resSelect is null");
        }

        SQLTable table = SQLTable.fromString(resSelect.getValue());
        System.out.println(table.toStringTable());

        assertTrue(ex == null) ;
        assertTrue(resInsert.getStatus() == StatusType.SQLINSERT_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("ValidStudent").get("student").equals("ValidStudent"));
        assertTrue(table.getRow("ValidStudent").get("age").equals("10"));
    }

    @Test
    public void testSQLUpdateInvalid() {
        this.logger.info("--- testSQLUpdateInvalid() ---");
        String tableName = "testSQLCreateValid";
        String invalidRow = "{\"ag--e\":999,\"stu  dent\":ValidStudent}"; // invalid colname
        BasicKVMessage resUpdate = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resUpdate = kvClient.sqlupdate(tableName, invalidRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resUpdate != null) {
            this.logger.info("resUpdate " + resUpdate.getStatus());
        } else {
            this.logger.info("resUpdate is null");
        }

        SQLTable table;
        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
        } else {
            this.logger.info("resSelect is null");
        }

        table = SQLTable.fromString(resSelect.getValue());
        System.out.println(table.toStringTable());

        assertTrue(ex == null) ;
        assertTrue(resUpdate.getStatus() == StatusType.SQLUPDATE_ERROR);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("ValidStudent").get("student").equals("ValidStudent"));
        assertTrue(table.getRow("ValidStudent").get("age").equals("10"));
    }

    @Test
    public void testSQLUpdateValid() {
        this.logger.info("--- testSQLUpdateValid() ---");
        String tableName = "testSQLCreateValid";
        String validRow = "{\"age\":888,\"student\":ValidStudent}"; 
        BasicKVMessage resUpdate = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resUpdate = kvClient.sqlupdate(tableName, validRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resUpdate != null) {
            this.logger.info("resUpdate " + resUpdate.getStatus());
        } else {
            this.logger.info("resUpdate is null");
        }

        SQLTable table;
        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
    } else {
            this.logger.info("resSelect is null");
        }

        table = SQLTable.fromString(resSelect.getValue());
        System.out.println(table.toStringTable());

        assertTrue(ex == null) ;
        assertTrue(resUpdate.getStatus() == StatusType.SQLUPDATE_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("ValidStudent").get("student").equals("ValidStudent"));
        assertTrue(table.getRow("ValidStudent").get("age").equals("888"));
    }

    @Test
    public void testSQLDropInvalid() {
        this.logger.info("--- testSQLDropInvalid() ---");
        String validTableName = "testSQLCreateValid";
        String invalidTableName = "somesqltablethatdoesntexist";
        BasicKVMessage resDrop = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(invalidTableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resDrop = kvClient.sqldrop(invalidTableName);
            resSelect = kvClient.sqlselect(validTableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resDrop != null) {
            this.logger.info("resDrop " + resDrop.getStatus());
        } else {
            this.logger.info("resDrop is null");
        }

        SQLTable table;
        if (resSelect != null) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
        } else {
            this.logger.info("resSelect is null");
        }

        assertTrue(ex == null) ;
        assertTrue(resDrop.getStatus() == StatusType.SQLDROP_ERROR);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(validTableName));
    }

    @Test
    public void testSQLDropValid() {
        this.logger.info("--- testSQLDropValid() ---");
        String tableName = "testSQLCreateValid";
        BasicKVMessage resDrop = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resDrop = kvClient.sqldrop(tableName);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        if (resDrop != null) {
            this.logger.info("resDrop " + resDrop.getStatus());
        } else {
            this.logger.info("resDrop is null");
        }

        SQLTable table;
        if (resSelect != null && resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS) {
            this.logger.info("resSelect");
            this.logger.info("Status: " + resSelect.getStatus());
            this.logger.info("Key: " + resSelect.getKey());
            this.logger.info("Value: " + resSelect.getValue());
            table = SQLTable.fromString(resSelect.getValue());
            System.out.println(table.toStringTable());
        } else {
            this.logger.info("resSelect is null");
        }

        assertTrue(ex == null) ;
        assertTrue(resDrop.getStatus() == StatusType.SQLDROP_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_ERROR);
        assertTrue(resSelect.getKey().equals(tableName));
    } 

    @Test
    public void testSQLReplicationOnUpdate() {
        this.logger.info("--- testSQLReplicationOnUpdate() ---");
        String tableName = "testSQLReplicationOnUpdate";
        String schema = "age:int,student:text";
        String initRow = "{\"age\":5,\"student\":a}"; 
        String updateRow = "{\"age\":100,\"student\":a}"; 
        BasicKVMessage resCreate = null;
        BasicKVMessage resInsert = null;
        BasicKVMessage resUpdate = null;
        BasicKVMessage resSelect = null;
        Exception ex = null;

        ECSNode node = getReponsibleNode(tableName);
        ECSNode[] replicas = getReplicas(node);

        kvClient = createKVClient(node.getNodeHost(), node.getNodePort());

        try {
            kvClient.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            resCreate = kvClient.sqlcreate(tableName, schema);
            resInsert = kvClient.sqlinsert(tableName, initRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        SQLTable table = SQLTable.fromString(resSelect.getValue());
        assertTrue(resCreate.getStatus() == StatusType.SQLCREATE_SUCCESS);
        assertTrue(resInsert.getStatus() == StatusType.SQLINSERT_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("a").get("student").equals("a"));
        assertTrue(table.getRow("a").get("age").equals("5"));

        try {
            resUpdate = kvClient.sqlupdate(tableName, updateRow);
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        table = SQLTable.fromString(resSelect.getValue());
        assertTrue(resUpdate.getStatus() == StatusType.SQLUPDATE_SUCCESS);
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("a").get("student").equals("a"));
        assertTrue(table.getRow("a").get("age").equals("100"));

        try {
            kvClient.reconnect(replicas[0].getNodeHost(), replicas[0].getNodePort());
            resSelect = kvClient.sqlselect(tableName, true);
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        table = SQLTable.fromString(resSelect.getValue());
        assertTrue(resSelect.getStatus() == StatusType.SQLSELECT_SUCCESS);
        assertTrue(resSelect.getKey().equals(tableName));
        assertTrue(table.getSize() == 1);
        assertTrue(table.getRow("a").get("student").equals("a"));
        assertTrue(table.getRow("a").get("age").equals("100"));
    }
}
