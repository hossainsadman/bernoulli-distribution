package testing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.Collections;

import app_kvECS.ECSClient;
import client.KVStore;
import ecs.ECSHashRing;
import ecs.ECSNode;
import logger.LogSetup;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;

public class PerfEnronTest {
    private static final String ENRON_DIR = "/cad2/ece419s/enron_mail_20150507/maildir/";
    private static final Logger logger = Logger.getLogger(PerfEnronTest.class);

    private static final String ECS_ADDR = "127.0.0.1";
    private static final int ECS_PORT = 20000;

    private static final int SERVER_PORT_START = 5000;
    private static final int NUM_NODES = 5;

    private static final String CACHE_STRAT = "FIFO";
    private static final int CACHE_fileNum = 10;

    private static ECSClient ecsClient;
    private static ArrayList<ECSNode> allNodes;
    public static KVStore kvClient;

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            // ecsClient = new ECSClient(ECS_ADDR, ECS_PORT);
            // ecsClient.setTesting(true);
            // ecsClient.start();

            // allNodes = new ArrayList<>(NUM_NODES);
            // for (int i = 0; i < NUM_NODES; i++) {
            //     allNodes.add(ecsClient.addNode(CACHE_STRAT, CACHE_fileNum, SERVER_PORT_START + i));
            // }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
	public static void tearDown() {
        // for (ECSNode node : allNodes) {
        //     ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
        //     try {
        //         TimeUnit.MILLISECONDS.sleep(500);
        //     } catch (InterruptedException e) {
        //         // TODO Auto-generated catch block
        //         e.printStackTrace();
        //     }
        // }

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

    private File[] loadEnron(int fileNum) {
        File[] files = null;
    
        try (Stream<Path> paths = Files.walk(Paths.get(ENRON_DIR))) {
            Stream<Path> fileStream = paths.parallel().filter(Files::isRegularFile);
            
            if (fileNum > 0) {
                fileStream = fileStream.limit(fileNum);
            }
    
            files = fileStream.map(Path::toFile).toArray(File[]::new);
        } catch (IOException e) {
            e.printStackTrace();
        }
    
        return files;
    }

    private void saveEnron(int fileNum, String saveFilePath) {
        try (Stream<Path> paths = Files.walk(Paths.get(ENRON_DIR));
            PrintWriter writer = new PrintWriter(new FileWriter(saveFilePath))) {
            
            Stream<Path> filteredPaths = paths.parallel().filter(Files::isRegularFile);
            
            if (fileNum > 0) {
                filteredPaths = filteredPaths.limit(fileNum);
            }
            
            filteredPaths.forEach(path -> writer.println(path.toString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }   

    private File[] loadCachedEnron(String loadFilePath) {
        Path path = Paths.get(loadFilePath);
        if (!Files.exists(path)) {
            saveEnron(-1, loadFilePath);  // save all files
            System.exit(0);
        }

        List<File> files = Collections.synchronizedList(new ArrayList<>());

        try (Stream<String> lines = Files.lines(Paths.get(loadFilePath))) {
            lines.parallel().forEach(line -> files.add(new File(line)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return files.toArray(new File[0]);
    }

    @Test
    public void PerfEnron() {
        PerfEnronTest test = new PerfEnronTest();
        File[] files = test.loadCachedEnron("enron_files.txt");
        System.out.println("Number of files: " + files.length);
        // print each file path but only 2 folders above from the actual file
        // for the first 10 files
        for (int i = 0; i < 10; i++) {
            String output = files[i].getParentFile().getParentFile().getName()
                          + files[i].getParentFile().getName()
                          + files[i].getName();
            if (output.length() > 20) {
                output = output.substring(output.length() - 20);
            }
            System.out.println(output);
        }
        assertTrue(1 == 1);
    }
}
