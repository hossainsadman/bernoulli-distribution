package ecs;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import org.apache.log4j.*;

import shared.messages.ECSMessage;


public class ServerConnection implements Runnable {
    private static Logger logger = Logger.getRootLogger();
    private Socket serverSocket;
    private ECSNode node;
    private ECS ecs;
    private boolean isOpen;

    ServerConnection(ECS ecs, Socket serverSocket){
        this.node = null;
        this.serverSocket = serverSocket;
        this.ecs = ecs;
        this.isOpen = true;
    }

    public void run() {
        while (isOpen) {
            if(this.serverSocket == null) isOpen = false;
            else{
                try {
                    Object receivedObject = readObjectFromSocket(this.serverSocket);
                    if (receivedObject == null) {
                        // Connection has been closed by ECS, handle gracefully
                        System.out.println("Socket connection closed, stopping listener.");
                        isOpen = false;
                        break;
                    }
                    // If receivedObject is not null, cast to ECSMessage and process further
                    ECSMessage message = (ECSMessage) receivedObject;
                    processMessage(message);
                } catch (Exception ioe) {
                    System.out.println(ioe);
                    isOpen = false;
                }
            }
        }
        shutdown();
    }

    public void shutdown() {
        if (serverSocket == null) return;
        HashMap<String, String> kvPairs = (HashMap<String, String>) readObjectFromSocket(this.serverSocket);
        System.out.println(kvPairs);
        if (node != null){
            ECSNode nextNode = this.ecs.hashRing.removeNode(node);
            if (nextNode != null && kvPairs != null && kvPairs.size() > 0){
                sendMessage(nextNode.getServerSocket(), ECSMessage.RECEIVE, null, kvPairs);
            }
            this.ecs.nodes.remove(this.node.getNodeName());
            this.ecs.sendMetadataToNodes();
        }

        System.out.println("Closing connection");
        try {
            if (serverSocket != null)
                serverSocket.close();

            logger.info("Connection closed for " + serverSocket.getInetAddress().getHostName());
            this.serverSocket = null;
        } catch (IOException e) {
            logger.error("Error! closing connection", e);
        }
    }

    private Object readObjectFromSocket(Socket socket) {
        Object obj = null;
        try {
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            obj = in.readObject();
        } catch(EOFException e){
            // Connection has been closed by ECS, handle gracefully
            System.out.println("Connection has been closed by the other side.");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

    private void writeObjectToSocket(Socket socket, Object obj) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendMessage(Socket socket, ECSMessage messageType, Object... data){
        writeObjectToSocket(socket, messageType);
        for(Object o: data) writeObjectToSocket(socket, o);
    }

    private void initServer(){
        logger.info("ECS connected to KVServer via " +  serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort());

        String serverName = (String) readObjectFromSocket(serverSocket);
        System.out.println("Connected : " + serverName);
        // split serverName by : to get the server address and port
        String[] serverInfo = serverName.split(":");
        String serverAddress = serverInfo[0];
        int serverPort = Integer.parseInt(serverInfo[1]);

        logger.info("ECS connected to KVServer at " + serverAddress + ":" + serverPort);

        ECSNode newNode = new ECSNode(serverName, serverAddress, serverPort, serverSocket);
        this.node = newNode;
        this.ecs.nodes.put(serverName, newNode); // append to the table
        this.ecs.setNodeAvailability(newNode, true); // set the node available

        // WRITE_LOCK on kvserver

        ECSNode oldNode = this.ecs.hashRing.addNode(newNode);
        logger.info("Added " + serverName + " to the hashring.");
        logger.info("KEYRANGE: " + this.ecs.hashRing.toString());

        this.ecs.sendMetadataToNodes();

        // oldNode is null if newNode is the only node in the hashring
        if (oldNode != null) {
            sendMessage(oldNode.getServerSocket(), ECSMessage.TRANSFER_FROM, newNode);
        }
    }

    private void transferKeys() {
        System.out.println("TRANSFER_TO Command");
        ECSNode toNode = (ECSNode) readObjectFromSocket(this.serverSocket);
        HashMap<String, String> kvPairs = (HashMap) readObjectFromSocket(this.serverSocket);

        if(kvPairs != null && kvPairs.size() > 0){
            logger.info("Transferring " + kvPairs.size() + " key-value pairs from " + this.node.getNodeName() + " to " + this.node.getNodeName());
            logger.info("kvPairs: " + kvPairs.toString());

            Socket toNodeSocket = this.ecs.hashRing.getNodeForIdentifier(toNode.getNodeIdentifier()).getServerSocket();
            sendMessage(toNodeSocket, ECSMessage.RECEIVE, this.node, kvPairs);
        }
    }

    private void handleTransferComplete() {
        System.out.println("TRANSFER_COMPLETE Command");
        ECSNode pingNode = (ECSNode) readObjectFromSocket(this.serverSocket);
        Socket pingNodeSocket = this.ecs.hashRing.getNodeForIdentifier(pingNode.getNodeIdentifier()).getServerSocket();
        sendMessage(pingNodeSocket, ECSMessage.TRANSFER_COMPLETE);
    }

    /**
     * KVServer <-> ECS message Protocol
     * 
     * INIT <server-name:String> 
     * - Called when KVServer connects to ECS
     * 
     * HASHRING <hashring:ECSHashRing>
     * - Update hashring on KVServer
     * 
     * TRANSFER_FROM <toNode:ECSNode>
     * - Sent from ECS -> KVServer to allow KVServer to tranfer nodes to specified toNode
     * 
     * TRANSFER_TO <toNode:ECSNode> <kvPairs:HashMap<String, String>>
     * - Sent from KVServer -> ECS to trigger transfer of kvPairs to toNode
     * - Sent after TRANSFER_FROM from the node tranferring kvPairs to the node receiving them
     * - TRANSFER_TO.toNode == TRANSFER_FROM.toNode
     * 
     * RECEIVE <fromNode:ECSNode> <kvPairs:HashMap<String, String>>
     * - Sent from ECS -> KVServer that is getting kvPairs tranfered to
     * - Sent after TRANSFER_TO command received from KVServer
     * - Sent is the fromNode that is transferring the kvPairs
     * 
     * TRANSFER_COMPLETE <pingNode:ECSNode>
     * - Sent after the keys have been transfered to a specific KVServer from pingServer, and pingServer is notified
     * - Sent after the KVServer processes the RECEIVE command
     * - TRANSFER_COMPLETE.pingNode == RECEIVE.fromNode
     */
    private void processMessage(ECSMessage message){
        switch (message){
            case INIT:
                initServer();
                break;
            case TRANSFER_TO:
                transferKeys();
                break;
            case SHUTDOWN:
                shutdown();
                break;
            case TRANSFER_COMPLETE:
                handleTransferComplete();
                break;
            default:
                System.out.println("Unrecognized Command " + message);
        }
    }
}
