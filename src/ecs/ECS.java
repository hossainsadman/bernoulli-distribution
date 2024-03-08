package ecs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.*;

import logger.LogSetup;
import shared.messages.BasicKVMessage;
import shared.messages.ECSMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import app_kvECS.ECSClient;
import app_kvServer.ClientConnection;

import ecs.ECSHashRing;

import org.apache.zookeeper.*;
/* 
    ECSClient should initialize ECS. 
    If needed, integrate zookeeper here.
*/

public class ECS {
    private static Logger logger;

    private static final String DEFAULT_ECS_ADDR = "127.0.0.1";
    private static final int DEFAULT_ECS_PORT = 9999;

    private String address;
    private int port;

    private ServerSocket ecsSocket;

    JSONObject config = null;;
    JSONTokener tokener = null;

    public ECSHashRing hashRing;

    /*
     * Integrity Constraint:
     * IECSNode in availableNodes = values of nodes
     */
    public Map<String, IECSNode> nodes = new HashMap<>(); /* maps server name -> node */
    private ArrayList<IECSNode> availableNodes = new ArrayList<>();
    private static final List<ServerConnection> connections = new CopyOnWriteArrayList<>();

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

        logger.info("ECS initialized at " + this.address + ":" + this.port);
    }

    public ECS(Logger logger) throws Exception{
        this.init_config("./ecs_config.json");
        if(this.config == null)
            throw new Exception("Config file not initialized");

        try {
            this.address = this.config.getJSONObject("ecs").getString("address");
            this.port = this.config.getJSONObject("ecs").getInt("port");
        } catch (JSONException e) {
            logger.error("Error reading config file.");
            e.printStackTrace();
        }

        this.address = DEFAULT_ECS_ADDR;
        this.port = DEFAULT_ECS_PORT;
        this.logger = logger;
        this.hashRing = new ECSHashRing();

        logger.info("ECS initialized at " + this.address + ":" + this.port);
    }

    private void init_config(String configPath){
        if(configPath == null) configPath = "./ecs_config.json";

        try {
            tokener = new JSONTokener(new FileInputStream("./ecs_config.json"));
            this.config = new JSONObject(tokener);
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
            logger.info("ECS is listening at " + address + ":" + port);

            Thread serverConnThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ECS.this._acceptServetConnections();
                }
            });

            serverConnThread.start();

            return true;
        } catch (IOException e) {
            logger.error("ECS Socket cannot be opened: ");
            // Could be a connection binding issue from server side
            if (e instanceof BindException)
                logger.error("Port " + port + " at address " + address + " is already bound.");
            return false;
        }
    }


    public void _acceptServetConnections() {
        if (ecsSocket == null) return;

        while (!ecsSocket.isClosed()) {
            Socket kvServerSocket = null;
            try {
                kvServerSocket = ecsSocket.accept();
                ServerConnection connection = new ServerConnection(this, kvServerSocket);
                connections.add(connection);
                new Thread(connection).start();
            } catch (SocketException se) {
                if (ecsSocket.isClosed()) {
                    logger.info("ServerSocket is closed.");
                    break;
                }
            } catch (IOException e) {
                logger.error("Unable to establish connection.\n", e);
            }
        }
    }

    private void sendMessageToNode(Socket socket, ECSMessage messageType, Object data){
        writeObjectToSocket(socket, messageType);
        if (data != null) writeObjectToSocket(socket, data);
    }

    public void sendMetadataToNodes(){
        for (ECSNode node : this.hashRing.getHashring().values()) {
            sendMessageToNode(node.getServerSocket(), ECSMessage.HASHRING, this.hashRing);
        }
    }

    public Object readObjectFromSocket(Socket socket) {
        Object obj = null;
        try {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            obj = in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

    public void writeObjectToSocket(Socket socket, Object obj) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        try {
            ecsSocket.close();
        } catch (IOException e) {
            logger.error("Unable to close socket at " + address + ":" + port, e);
        }
    }

    public void stop() {
        try {
            for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
                ECSNode node = (ECSNode) entry.getValue();
                nodes.remove(entry);
                node.closeConnection();
            }
        } catch (Exception e) {
            logger.error("Error closing connection", e);
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

    // public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
    //     if (count > availableNodes.size()) {
    //         throw new IllegalArgumentException("Not enough available servers to fulfill the request.");
    //     }

    //     Collection<IECSNode> addedNodes = new ArrayList<>();

    //     for (int i = 0; i < count; ++i) {
    //         /* Select and remove a server from the pool */
    //         IECSNode newServer = availableNodes.get(0);

    //         if (initServer((ECSNode) newServer, cacheStrategy, cacheSize))
    //             addedNodes.add(newServer);

    //         // update Zookeeper or another coordination service here if needed
    //     }

    //     // Rebalance the key space among all nodes
    //     // rebalanceKeyspace();

    //     return addedNodes;
    // }

    // public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
    //     // TODO
    //     return null;
    // }

    // public boolean awaitNodes(int count, int timeout) throws Exception {
    //     // TODO
    //     return false;
    // }

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
    //             logger.error("Error closing connection with server " + nodeName, e);
    //         }
    //     }

    //     return removedAll;
    // }

    public Map<String, IECSNode> getNodes() {
        return this.nodes;
    }

    public IECSNode getNodeByServerName(String serverName) {
        return nodes.get(serverName);
    }

    public static int getDefaultECSPort() {
        return DEFAULT_ECS_PORT;
    }

    public static String getDefaultECSAddr() {
        return DEFAULT_ECS_ADDR;
    }
}
