package testing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.time.LocalDateTime;
import java.util.concurrent.*;

import app_kvECS.ECSClient;
import app_kvClient.KVClient;
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

    private static final int NUM_NODES = 1;
    private static final int NUM_CLIENTS = 1;

    private static final String CACHE_STRAT = "FIFO";
    private static final int CACHE_SIZE = 10;

    private static final int TOTAL_PAIRS = 1000;

    private static final int[] NUM_NODES_VALS = {1,5,10,15,20};
    private static final int[] NUM_CLIENTS_VALS = {1,5,20,50,100};
    private static final String[] CACHE_STRAT_VALS = {"FIFO", "LFU", "LRU", "None"};
    private static final int[] CACHE_SIZE_VALS = {0,5,20,50,100};

    private static ECSClient ecsClient;
    private static ArrayList<ECSNode> allNodes;
    private static ArrayList<KVStore> allClients;

    private static Random rand = new Random();

    long startTime;
    long endTime;

    private static File[] files = null;

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
            ecsClient = new ECSClient(ECS_ADDR, ECS_PORT);
            ecsClient.setTesting(true);
            ecsClient.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setupNodes(int numNodes, String cacheStrat, int cacheSize) {
        allNodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            allNodes.add(ecsClient.addNode(cacheStrat, cacheSize, SERVER_PORT_START + i));
        }
        // print all nodes
        for (ECSNode node : allNodes) {
            logger.info("Node: " + node.getNodeHost() + ":" + node.getNodePort());
        }
    }

    private void setupClients(int numClients, int numNodes) {
        // print all nodes
        for (ECSNode node : allNodes) {
            logger.info("Node: " + node.getNodeHost() + ":" + node.getNodePort());
        }

        allClients = new ArrayList<>(numClients);
        for (int i = 0; i < numClients; i++) {
            int nodeIndex = i % numNodes;
            ECSNode node = allNodes.get(nodeIndex);
            assert(node != null);
            String host = node.getNodeHost();
            assert(host != null);
            int port = node.getNodePort();
            assert(port > 0);

            KVStore kvClient = createKVClient(allNodes.get(nodeIndex).getNodeHost(), allNodes.get(nodeIndex).getNodePort());
            try {
                kvClient.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            allClients.add(kvClient);
        }
    }

    private void connectClients() {
        for (KVStore kvClient : allClients) {
            try {
                kvClient.connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @After
	public void tearDown() {
        // print allnodes
        for (ECSNode node : allNodes) {
            logger.info("Node: " + node.getNodeHost() + ":" + node.getNodePort());
        }
        for (ECSNode node : allNodes) {
            ecsClient.removeNodes(Arrays.asList(node.getNodeHost() + ":" + node.getNodePort()));
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // print allnodes
        for (ECSNode node : allNodes) {
            logger.info("Node: " + node.getNodeHost() + ":" + node.getNodePort());
        }

        for (KVStore kvClient : allClients) {
            kvClient.disconnect();
        }

        allNodes.clear();
        allClients.clear();
    }

    private static void createNode(int port){
        ECSNode node = ecsClient.addNode("FIFO", 10, port);
        allNodes.add(node);
    }

    private static KVStore createKVClient(String host, int port) {
        // if (host == null) {
        //     host = allNodes.get(0).getNodeHost();
        //     port = allNodes.get(0).getNodePort();
        // }
    
        KVStore kvClient = new KVStore(host, port);
        kvClient.setTesting(false);
        kvClient.setMaxRetries(5);
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

    private static String fileToKey(File file) {
        String output = file.getParentFile().getParentFile().getName()
                      + file.getParentFile().getName()
                      + file.getName();
        if (output.length() > 20) {
            output = output.substring(output.length() - 20);
        }
        return output;
    }

    public String fileToVal(File file) {
        StringBuilder sb = new StringBuilder();
        try (Stream<String> stream = Files.lines(file.toPath(), StandardCharsets.UTF_8)) {
            stream.forEach(line -> sb.append(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public void putToClient(KVStore kvClient, String key, String value) {
        try {
            logger.info("Putting key: " + key);
            BasicKVMessage response = kvClient.put(key, value);
            // logger.info("Response: " + response.getStatus());
        } catch (Exception e) {
            logger.error("Error! while processing message", e);
        }
    }

    public void getFromClient(KVStore kvClient, String key) {
        try {
            logger.info("Getting key: " + key);
            BasicKVMessage response = kvClient.get(key);
            // logger.info("Response: " + response.getStatus());
        } catch (Exception e) {
            logger.error("Error! while processing message", e);
        }
    }

    public void putToClientEarlyExit(KVStore kvClient, String key, String value) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<BasicKVMessage> future = executor.submit(() -> {
            try {
                return kvClient.put(key, value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    
        try {
            BasicKVMessage response = future.get(500, TimeUnit.MILLISECONDS);
            // logger.info("Response: " + response.getStatus());
        } catch (TimeoutException e) {
            logger.info("Execution took longer than 500 ms. Returning...");
            return;
        } catch (Exception e) {
            logger.error("Error! while processing message", e);
        } finally {
            executor.shutdownNow();
        }
    }
    
    public void getFromClientEarlyExit(KVStore kvClient, String key) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<BasicKVMessage> future = executor.submit(() -> {
            try {
                return kvClient.get(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    
        try {
            BasicKVMessage response = future.get(500, TimeUnit.MILLISECONDS);
            // logger.info("Response: " + response.getStatus());
        } catch (TimeoutException e) {
            logger.info("Execution took longer than 500 ms. Returning...");
            return;
        } catch (Exception e) {
            logger.error("Error! while processing message", e);
        } finally {
            executor.shutdownNow();
        }
    }
    
    public void putToRandomClient(int startIndex, int numValues) {
        for (int i = startIndex; i < startIndex + numValues && i < files.length; i++) {
            String key = fileToKey(files[i]);
            String value = fileToVal(files[i]);
            KVStore kvClient = allClients.get(rand.nextInt(allClients.size()));
            putToClient(kvClient, key, value);
        }
    }
    
    public void getFromRandomClient(int startIndex, int numValues) {
        for (int i = startIndex; i < startIndex + numValues && i < files.length; i++) {
            String key = fileToKey(files[i]);
            KVStore kvClient = allClients.get(rand.nextInt(allClients.size()));
            getFromClient(kvClient, key);
        }
    }

    public void runClientsSimultaneously(int startIndex, int numValues, List<KVStore> allClients) {
        Thread putThread = new Thread(() -> putToRandomClient(startIndex, numValues));
        Thread getThread = new Thread(() -> {
            try {
                // Delay the start of getFromRandomClient by a 2 s
                Thread.sleep(2000);
                getFromRandomClient(startIndex, numValues);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    
        putThread.start();
        getThread.start();
    
        try {
            // Wait for both threads to finish
            putThread.join();
            getThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void putToEachClient(int numPairsPerClient) {
        int totalPairs = numPairsPerClient * allClients.size();
        for (int i = 0; i < totalPairs || i < files.length; i++) {
            String key = fileToKey(files[i]);
            String value = fileToVal(files[i]);
            KVStore kvClient = allClients.get(i / numPairsPerClient);
            putToClient(kvClient, key, value);
        }
    }
    
    public void getFromEachClient(int numPairsPerClient) {
        int totalPairs = numPairsPerClient * allClients.size();
        for (int i = 0; i < totalPairs || i < files.length; i++) {
            String key = fileToKey(files[i]);
            KVStore kvClient = allClients.get(i / numPairsPerClient);
            getFromClient(kvClient, key);
        }
    }

    public long calculateTimeTaken(long startTime, long endTime) {
        return ((endTime - startTime) / 1_000_000);
    }

    public void timeTaken(long startTime, long endTime, int totalPairs, int numPairsPerClient, int numNodes, long timeForSetupNodes,
                          int numClients, String cacheStrat, int cacheSize, long timeForPutRequests, long timeForGetRequests) {
        double requestsPerSecond = (totalPairs * 2.0) / ((double)timeForGetRequests / 1000 + (double)timeForPutRequests / 1000);
        double avgLatencyPerPutRequest = (double)totalPairs / (double)timeForPutRequests / 1000;
        double avgLatencyPerGetRequest = (double)totalPairs / (double)timeForGetRequests / 1000;

        String message = "TIMESTAMP: " + LocalDateTime.now() + "\n" +
            "Total pairs: " + totalPairs + "\n" +
            "Num pairs per client: " + numPairsPerClient + "\n" +
            "Time taken for put requests: " + timeForPutRequests + " ms" + "\n" +
            "Time taken for get requests: " + timeForPutRequests + " ms" + "\n" +
            "Time taken for setup nodes: " + timeForSetupNodes + " ms" + "\n" +
            "NUM_NODES: " + numNodes + "\n" +
            "NUM_CLIENTS: " + numClients + "\n" +
            "CACHE_STRAT: " + cacheStrat + "\n" +
            "CACHE_SIZE: " + cacheSize + "\n" +
            "Requests per second: " + requestsPerSecond + "\n" +
            "Average latency per put request: " + avgLatencyPerPutRequest + " ms" + "\n" +
            "Average latency per get request: " + avgLatencyPerGetRequest + " ms" + "\n" +
            "------------------------------------------";
        logger.info(message);

        // Write to perftest.txt
        try (PrintWriter out = new PrintWriter(new FileOutputStream("perftest.txt", true))) {
            out.println(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // @Test
    // public void ExampleTest() {
    //     PerfEnronTest test = new PerfEnronTest();
    //     int numPairsPerClient = TOTAL_PAIRS / NUM_CLIENTS;

    //     startTime = System.nanoTime();
    //     setupNodes(NUM_NODES, CACHE_STRAT, CACHE_SIZE);
    //     endTime = System.nanoTime();
    //     long timeForSetupNodes = calculateTimeTaken(startTime, endTime);

    //     setupClients(NUM_CLIENTS, NUM_NODES);
    //     connectClients();

    //     files = test.loadCachedEnron("enron_files.txt");     
        
    //     startTime = System.nanoTime();
    //     putToRandomClient(0, TOTAL_PAIRS);
    //     endTime = System.nanoTime();
    //     long timeForPutRequests = calculateTimeTaken(startTime, endTime);

    //     startTime = System.nanoTime();
    //     getFromRandomClient(0, TOTAL_PAIRS);
    //     endTime = System.nanoTime();
    //     long timeForGetRequests = calculateTimeTaken(startTime, endTime);

    //     timeTaken(startTime, endTime, TOTAL_PAIRS, numPairsPerClient, NUM_NODES, timeForSetupNodes, NUM_CLIENTS, CACHE_STRAT, CACHE_SIZE, timeForPutRequests, timeForGetRequests);
    //     tearDown();
    // }    

    @Test
    public void runTestForSpecificValues() {
        int numNodes        = 5;
        int numClients      = 5;
        String cacheStrat   = "LRU";
        int cacheSize       = 100;

        PerfEnronTest test = new PerfEnronTest();
        int numPairsPerClient = TOTAL_PAIRS / numClients;

        long startTime = System.nanoTime();
        setupNodes(numNodes, cacheStrat, cacheSize);
        long endTime = System.nanoTime();
        long timeForSetupNodes = calculateTimeTaken(startTime, endTime);

        setupClients(numClients, numNodes);
        connectClients();

        files = test.loadCachedEnron("enron_files.txt");

        startTime = System.nanoTime();
        putToRandomClient(0, TOTAL_PAIRS);
        endTime = System.nanoTime();
        long timeForPutRequests = calculateTimeTaken(startTime, endTime);

        startTime = System.nanoTime();
        getFromRandomClient(0, TOTAL_PAIRS);
        endTime = System.nanoTime();
        long timeForGetRequests = calculateTimeTaken(startTime, endTime);

        timeTaken(startTime, endTime, TOTAL_PAIRS, numPairsPerClient, numNodes, timeForSetupNodes, numClients, cacheStrat, cacheSize, timeForPutRequests, timeForGetRequests);
        tearDown();
    }

    // @Test
    // public void AllTests() {
    //     PerfEnronTest test = new PerfEnronTest();
    //     int TOTAL_PAIRS = 100;
    
    //     for (int numNodes : NUM_NODES_VALS) {
    //         for (int numClients : NUM_CLIENTS_VALS) {
    //             for (String cacheStrat : CACHE_STRAT_VALS) {
    //                 for (int cacheSize : CACHE_SIZE_VALS) {
    //                     int numPairsPerClient = TOTAL_PAIRS / numClients;
    
    //                     long startTime = System.nanoTime();
    //                     setupNodes(numNodes, cacheStrat, cacheSize);
    //                     long endTime = System.nanoTime();
    //                     long timeForSetupNodes = calculateTimeTaken(startTime, endTime);
    
    //                     setupClients(numClients, numNodes);
    //                     connectClients();
    
    //                     files = test.loadCachedEnron("enron_files.txt");
    
    //                     startTime = System.nanoTime();
    //                     putToRandomClient(0, TOTAL_PAIRS);
    //                     endTime = System.nanoTime();
    //                     long timeForPutRequests = calculateTimeTaken(startTime, endTime);
    
    //                     startTime = System.nanoTime();
    //                     getFromRandomClient(0, TOTAL_PAIRS);
    //                     endTime = System.nanoTime();
    //                     long timeForGetRequests = calculateTimeTaken(startTime, endTime);
    
    //                     timeTaken(startTime, endTime, TOTAL_PAIRS, numPairsPerClient, numNodes, timeForSetupNodes, numClients, cacheStrat, cacheSize, timeForPutRequests, timeForGetRequests);
    //                     tearDown();
    //                 }
    //             }
    //         }
    //     }
    // }
}