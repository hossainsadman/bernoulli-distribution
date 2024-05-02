package ecs;

import org.apache.log4j.Logger;
import org.json.*;

import shared.messages.ECSMessage.ECSMessageType;
import shared.messages.MessageService;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import shared.ConsoleColors;

/* 
    ECSClient should initialize ECS. 
    If needed, integrate zookeeper here.
*/

public class ECS {
    private MessageService messageService = new MessageService();
    private static Logger logger;

    private static final String DEFAULT_ECS_ADDR = "127.0.0.1";
    private static final int DEFAULT_ECS_PORT = 9999;
    private static final int TIMEOUT_AWAIT_NODES = 5000;

    private String address;
    private int port;

    private ServerSocket ecsSocket;

    JSONObject config = null;;
    JSONTokener tokener = null;

    public ECSHashRing hashRing;
    public boolean testing = false;

    /*
     * Integrity Constraint:
     * IECSNode in availableNodes = values of nodes
     */
    public Map<String, ECSNode> nodes = new HashMap<>(); /* maps server name -> node */
    private HashSet<Integer> availablePorts = new HashSet<Integer>();
    private ArrayList<ECSNode> availableNodes = new ArrayList<>();
    public static final List<ServerConnection> connections = new CopyOnWriteArrayList<>();

    public ECS(String address, int port, Logger logger) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");

        this.address = address;
        this.port = port;
        this.logger = logger;
        this.hashRing = new ECSHashRing();

        this.init_config("./ecs_config.json");
    }

    public ECS(String address, int port) { // no logger
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");

        this.address = address;
        this.port = port;
        this.hashRing = new ECSHashRing();

        this.init_config("./ecs_config.json");

        logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "ECS initialized at " + this.address + ":" + this.port + ConsoleColors.RESET);
    }

    public ECS(Logger logger) throws Exception{
        this.init_config("./ecs_config.json");

        this.address = DEFAULT_ECS_ADDR;
        this.port = DEFAULT_ECS_PORT;
        this.logger = logger;
        this.hashRing = new ECSHashRing();

        logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "ECS initialized at " + this.address + ":" + this.port + ConsoleColors.RESET);
    }

    private void init_config(String configPath){
        if(configPath == null) configPath = "./ecs_config.json";
        try {
            tokener = new JSONTokener(new FileInputStream("./ecs_config.json"));
            this.config = new JSONObject(tokener);
            this.address = this.config.getJSONObject("ecs").getString("address");
            this.port = this.config.getJSONObject("ecs").getInt("port");
            JSONArray portsArray = this.config.getJSONArray("ports");
            this.availablePorts = IntStream.range(0, portsArray.length())
                                    .mapToObj(portsArray::getInt)
                                    .collect(Collectors.toCollection(HashSet::new));
        } catch (FileNotFoundException e) {
            logger.warn("No ecs_config.json file found.");
        } catch (Exception e) {
            System.err.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public boolean start() {
        try {
            ecsSocket = new ServerSocket(port, 10, InetAddress.getByName(address));
            logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "ECS is listening at " + address + ":" + port + ConsoleColors.RESET);

            Thread serverConnThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ECS.this._acceptServetConnections();
                }
            });

            serverConnThread.start();

            return true;
        } catch (IOException e) {
            logger.error(ConsoleColors.RED_UNDERLINED + "ECS Socket cannot be opened: " + ConsoleColors.RESET);
            // Could be a connection binding issue from server side
            if (e instanceof BindException)
                logger.error(ConsoleColors.RED_UNDERLINED + "Port " + port + " at address " + address + " is already bound." + ConsoleColors.RESET);
            return false;
        }
    }

    public void setTesting(boolean testing) {
        this.testing = testing;
    }


    public void _acceptServetConnections() {
        if (ecsSocket == null) return;

        while (!ecsSocket.isClosed()) {
            Socket kvServerSocket = null;
            try {
                kvServerSocket = ecsSocket.accept();
                ObjectOutputStream outputStream = new ObjectOutputStream(kvServerSocket.getOutputStream());
                outputStream.flush();
                ServerConnection connection = new ServerConnection(this, kvServerSocket, outputStream);
                connections.add(connection);
                new Thread(connection).start();
            } catch (SocketException se) {
                if (ecsSocket.isClosed()) {
                    logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "ServerSocket is closed." + ConsoleColors.RESET);
                    break;
                }
            } catch (IOException e) {
                logger.error(ConsoleColors.RED_UNDERLINED + "Unable to establish connection.\n" + ConsoleColors.RESET, e);
            }
        }
    }

    public void sendMetadataToNodes() {
        try {
            for (ECSNode node : hashRing.getHashring().values()) {
                if (!this.testing){
                    System.out.println(ConsoleColors.PURPLE_UNDERLINED + "\n\nSending to " + node.getNodeName() + " \n\nFrom ECS: " + hashRing.toString() + "\n" + ConsoleColors.RESET);
                }
                messageService.sendECSMessage(node.getServerSocket(), node.getObjectOutputStream(), ECSMessageType.HASHRING, "HASHRING", hashRing);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        try {
            ecsSocket.close();
        } catch (IOException e) {
            logger.error(ConsoleColors.RED_UNDERLINED + "Unable to close socket at " + address + ":" + port + ConsoleColors.RESET, e);
        }
    }

    public void stop() {
        try {
            for (Map.Entry<String, ECSNode> entry : nodes.entrySet()) {
                ECSNode node = (ECSNode) entry.getValue();
                nodes.remove(entry);
                node.closeConnection();
            }
        } catch (Exception e) {
            logger.error(ConsoleColors.RED_UNDERLINED + "Error closing connection" + ConsoleColors.RESET, e);
        }

        this.shutdown();
    }

    /*
     * Set the node's availability to true (in availableNodes) or false (rm from
     * availableNodes)
     */
    public void setNodeAvailability(String nodeIdentifier, boolean isAvailable) {
        if (nodes.containsKey(nodeIdentifier)) {
            ECSNode node = (ECSNode) nodes.get(nodeIdentifier);
            if (isAvailable && !availableNodes.contains(node)) {
                availableNodes.add(node);
                availableNodes.remove(node);
            } else
                availableNodes.remove(node);
        }
    }

    public void setNodeAvailability(ECSNode node, boolean isAvailable) {
        if (isAvailable && !availableNodes.contains(node)) {
            availableNodes.add(node);
            availableNodes.remove(node);
        } else
            availableNodes.remove(node);
    }

    public int startKVServer(String cacheStrategy, int cacheSize, int serverPort){
        if (this.availablePorts.isEmpty()) return -1;

        Integer port;
        if (serverPort == -1){
            Iterator<Integer> iterator = this.availablePorts.iterator();
            port = iterator.next();
            availablePorts.remove(port); // Remove the element from the set
        } else {
            port = serverPort;
        }
        String[] command = {"java", "-jar", "eagerDB-server.jar", "-p", port.toString(), "-c", String.valueOf(cacheSize), "-s",  cacheStrategy};

        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            if (!this.testing){
                builder.inheritIO();
            }
            builder.start();
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + e + ConsoleColors.RESET);
            e.printStackTrace();
        }

        return port;
    }

    public ECSHashRing getHashRing() {
        return hashRing;
    }

    public ECSNode getNodeByPort(int port) {
        for (ECSNode node : hashRing.getHashring().values()) {
            if (node.getNodePort() == port) {
                return node;
            }
        }
        return null;
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        for(int i = 0; i < count; i++){
            this.addNode(cacheStrategy, cacheSize);
        }
        return null;
    }

    public ECSNode addNode(String cacheStrategy, int cacheSize, int port) {
        int newCount = connections.size() + 1;
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Adding node " + newCount + " to ECS" + ConsoleColors.RESET);
        int serverPort = this.startKVServer(cacheStrategy, cacheSize, port);
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Started server on port " + serverPort + ConsoleColors.RESET);
        try {
            this.awaitNodes(newCount, TIMEOUT_AWAIT_NODES);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.getNodeByPort(serverPort);
    }

    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        int newCount = connections.size() + 1;
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Adding node " + newCount + " to ECS" + ConsoleColors.RESET);
        int serverPort = this.startKVServer(cacheStrategy, cacheSize, -1);
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Started server on port " + serverPort + ConsoleColors.RESET);
        try {
            this.awaitNodes(newCount, TIMEOUT_AWAIT_NODES);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.getNodeByPort(serverPort);
    }

    public ECSNode addNode(ECSNode node){
        logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "ECS connected to KVServer at " + node.getNodeHost() + ":" + node.getNodePort() + ConsoleColors.RESET);
        availablePorts.remove(node.getNodePort());

        this.nodes.put(node.getNodeName(), node); // append to the table
        this.setNodeAvailability(node, true); // set the node available

        ECSNode oldNode = this.hashRing.addNode(node);
        logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "Added " + node.getNodeName() + " to the hashring." + ConsoleColors.RESET);
        logger.info(ConsoleColors.PURPLE_BOLD_UNDERLINED + "KEYRANGE: " + this.hashRing.toString() + ConsoleColors.RESET);

        this.sendMetadataToNodes();

        return oldNode;
    }

    public boolean removeNodes(Collection<String> nodeNames){
        try {
            for(String name: nodeNames){
                int prevNodeCount = this.nodes.size();
                ECSNode node = this.nodes.get(name);
                if (node != null){
                    messageService.sendECSMessage(node.getServerSocket(), node.getObjectOutputStream(), ECSMessageType.SHUTDOWN_SERVER);
                    this.awaitNodes(prevNodeCount - 1, 1800);
                }
            }
            this.sendMetadataToNodes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public synchronized ECSNode removeNode(ECSNode node) {
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Removing NODE..." + ConsoleColors.RESET);
        ECSNode nextNode = this.hashRing.removeNode(node);
        this.nodes.remove(node.getNodeName());
        this.sendMetadataToNodes();

        return nextNode;
    }


    public boolean awaitNodes(int count, int timeout) throws Exception {
        long start = System.currentTimeMillis();
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "Before await " + connections.size() + ConsoleColors.RESET);
        while (System.currentTimeMillis() - start < timeout) {}
        System.out.println(ConsoleColors.PURPLE_UNDERLINED + "after await " + connections.size() + ConsoleColors.RESET);

        if(connections.size() == count) return true;
        throw new Exception(ConsoleColors.RED_UNDERLINED + "Await nodes timeout expired" + ConsoleColors.RESET);
    }

    // public boolean removeNodes(Collection<String> nodeNames) {
    //     boolean removedAll = true;

    //     for (String nodeName : nodeNames) {
    //         ECSNode removedNode = (ECSNode) nodes.remove(nodeName);

    //         // Node not found in the server name to ECSNode map
    //         if (removedNode == null) {
    //             removedAll = false;
    //             continue;
    //         }

    //         try {
    //             removedNode.closeConnection();
    //         } catch (Exception e) {
    //             logger.error(ConsoleColors.RED_UNDERLINED + "Error closing connection with server " + nodeName + ConsoleColors.RESET, e);
    //         }
    //     }

    //     return removedAll;
    // }

    public Map<String, ECSNode> getNodes() {
        return this.nodes;
    }

    public ECSNode getNodeByServerName(String serverName) {
        return nodes.get(serverName);
    }

    public static int getDefaultECSPort() {
        return DEFAULT_ECS_PORT;
    }

    public static String getDefaultECSAddr() {
        return DEFAULT_ECS_ADDR;
    }
}
