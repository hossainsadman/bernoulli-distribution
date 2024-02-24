package ecs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import java.io.IOException;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

import app_kvECS.ECSClient;

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
    private Map<String, ECSNode> nodes = new HashMap<>(); /* maps node identifier -> node */
    private ArrayList<ECSNode> availableNodes; // Indexable set
    private ArrayList<ECSNode> unavailableNodes;
    private int count = 0;

    public ECS(String address, int port, Logger logger) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");

        this.address = address;
        this.port = (port == -1) ? DEFAULT_ECS_PORT : port;
        this.logger = logger;
    }

    public boolean start() {
        try {
            ecsSocket = new ServerSocket(port, 10, InetAddress.getByName(address));
            logger.info("ECS is listening at " + address + ":" + port);
            return true;
        } catch (IOException e) {
            logger.error("ECS Socket cannot be opened: ");
            if (e instanceof BindException)
                logger.error("Port " + port + " at address " + address + " is already bound.");
            return false;
        }
    }

    public void run() { // TODO: fix
        if (ecsSocket != null) {
            while (true) {
                try {
                    Socket kvServerSocket = ecsSocket.accept();
                    // ClientConnection connection = new ClientConnection(this, clientSocket);
                    // connections.add(connection);
                    // new Thread(connection).start();
                    logger.info("<<< INSERT NEW ECS-SERVER CONNECTION HERE >>>");
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
        // TODO
        kill();
    }

    public boolean initServer(ECSNode newServer, String cacheStrategy, int cacheSize) {
        try {
            newServer.setCacheStrategy(cacheStrategy);
            newServer.setCacheSize(cacheSize);
            availableNodes.add(newServer);
            return true;
        } catch (Exception e) {
            logger.error("Can't initialize server: ", e);
        }
        return false;
    }

    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > availableNodes.size()) {
            throw new IllegalArgumentException("Not enough available servers to fulfill the request.");
        }

        List<IECSNode> addedNodes = new ArrayList<>();

        for (int i = 0; i < count; ++i) {
            /* Select and remove a server from the pool */
            ECSNode newServer = availableNodes.remove(0);
            
            initServer(newServer, cacheStrategy, cacheSize);

            if (newServer != null) {
                unavailableNodes.add(newServer);
                addedNodes.add(newServer);

                // update Zookeeper or another coordination service here
            }
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
        // TODO
        return null;
    }

    public static int getDefaultECSPort() {
        return DEFAULT_ECS_PORT;
    }
}
