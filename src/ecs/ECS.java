package ecs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import shared.messages.BasicKVMessage;

import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import app_kvECS.ECSClient;
import app_kvServer.ClientConnection;

import org.apache.zookeeper.*;
/* 
    ECSClient should initialize ECS. 
    If needed, integrate zookeeper here.
*/

public class ECS {
    private static Logger logger;

    private static final int DEFAULT_ECS_PORT = 9999;

    private String address;
    private int port = -1;

    private ServerSocket ecsSocket;

    /*
     * Integrity Constraint:
     * IECSNode in availableNodes and unavailableNodes = values of nodes
     */
    private Map<String, IECSNode> nodes = new HashMap<>(); /* maps server name -> node */
    private ArrayList<IECSNode> availableNodes;
    private ArrayList<IECSNode> unavailableNodes;

    public ECS(String address, int port, Logger logger) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");

        this.address = address;
        this.port = (port == -1) ? DEFAULT_ECS_PORT : port;
        this.logger = logger;

        /* zookeeper if needed */
    }

    public boolean start() {
        try {
            ecsSocket = new ServerSocket(port, 10, InetAddress.getByName(address));
            logger.info("ECS is listening at " + address + ":" + port);

            Thread serverConnThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ECS.this.acceptServerConnections();
                }
            });

            serverConnThread.start();

            return true;
        } catch (IOException e) {
            logger.error("ECS Socket cannot be opened: ");
            if (e instanceof BindException)
                logger.error("Port " + port + " at address " + address + " is already bound.");
            return false;
        }
    }

    public void acceptServerConnections() {
        if (ecsSocket != null) {
            while (true) {
                try {
                    logger.info("<<< INSERT NEW ECS-SERVER CONNECTION HERE >>>");
                    Socket kvServerSocket = ecsSocket.accept();
                    String serverAddress = kvServerSocket.getInetAddress().getHostAddress();
                    int serverPort = kvServerSocket.getPort();
                    String serverName = serverAddress + ":" + Integer.toString(serverPort);
                    ECSNode newNode = new ECSNode(serverName, serverAddress, serverPort, kvServerSocket);
                    nodes.put(serverName, newNode); // append to the table
                    setNodeAvailability(newNode, true); // set the node available

                    logger.info("Connected to " + kvServerSocket.getInetAddress().getHostName() + " on port "
                            + kvServerSocket.getPort());
                } catch (IOException e) {
                    logger.error("Unable to establish connection.\n", e);
                }
            }
        }
    }

    public void kill() {
        try {
            ecsSocket.close();
        } catch (IOException e) {
            logger.error("Unable to close socket at " + address + ":" + port, e);
        }
    }

    public void close() {
        try {
            for (Map.Entry<String, IECSNode> entry : nodes.entrySet()) {
                ECSNode node = (ECSNode) entry.getValue();
                nodes.remove(entry);
                node.closeConnection();
            }
        } catch (Exception e) {
            logger.error("Error closing connection", e);
        }
    
        kill();
    }

    public boolean initServer(ECSNode newServer, String cacheStrategy, int cacheSize) {
        try {
            newServer.setCacheStrategy(cacheStrategy);
            newServer.setCacheSize(cacheSize);
            this.setNodeAvailability(newServer, false);
            return true;
        } catch (Exception e) {
            logger.error("Can't initialize server: ", e);
        }
        return false;
    }

    /*
     * Set the node's availability to true (in availableNodes) or false (in
     * unavailableNodes)
     */
    private void setNodeAvailability(String nodeIdentifier, boolean isAvailable) {
        if (nodes.containsKey(nodeIdentifier)) {
            ECSNode node = (ECSNode) nodes.get(nodeIdentifier);
            if (isAvailable) {
                if (!availableNodes.contains(node)) {
                    availableNodes.add(node);
                    unavailableNodes.remove(node);
                }
            } else {
                if (!unavailableNodes.contains(node)) {
                    unavailableNodes.add(node);
                    availableNodes.remove(node);
                }
            }
        }
    }

    private void setNodeAvailability(ECSNode node, boolean isAvailable) {
        if (isAvailable) {
            if (!availableNodes.contains(node)) {
                availableNodes.add(node);
                unavailableNodes.remove(node);
            }
        } else {
            if (!unavailableNodes.contains(node)) {
                unavailableNodes.add(node);
                availableNodes.remove(node);
            }
        }
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > availableNodes.size()) {
            throw new IllegalArgumentException("Not enough available servers to fulfill the request.");
        }

        Collection<IECSNode> addedNodes = new ArrayList<>();

        for (int i = 0; i < count; ++i) {
            /* Select and remove a server from the pool */
            IECSNode newServer = availableNodes.get(0);

            if (initServer((ECSNode) newServer, cacheStrategy, cacheSize))
                addedNodes.add(newServer);

            // update Zookeeper or another coordination service here if needed
        }

        // Rebalance the key space among all nodes
        // rebalanceKeyspace();

        return addedNodes;
    }

    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    public Map<String, IECSNode> getNodes() {
        return this.nodes;
    }

    public static int getDefaultECSPort() {
        return DEFAULT_ECS_PORT;
    }
}
