package ecs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.*;

import shared.messages.ECSMessage;
import shared.messages.ECSMessage.ECSMessageType;
import shared.messages.MessageService;


public class ServerConnection implements Runnable {
    private MessageService messageService = new MessageService();
    private static Logger logger = Logger.getRootLogger();
    private Socket serverSocket;
    private ECSNode node;
    private ECS ecs;
    private boolean isOpen;
    private ObjectInputStream inStream;
    private ObjectOutputStream outStream;

    ServerConnection(ECS ecs, Socket serverSocket, ObjectOutputStream outStream){
        this.node = null;
        this.serverSocket = serverSocket;
        this.ecs = ecs;
        this.isOpen = true;
        this.outStream = outStream;
        try {
            this.inStream = new ObjectInputStream(serverSocket.getInputStream());
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void run() {
        while (isOpen) {
            if(this.serverSocket == null) isOpen = false;
            else{
                try {
                    ECSMessage message = messageService.receiveECSMessage(this.serverSocket, this.inStream);
                    if (message == null) {
                        isOpen = false;
                        break;
                    }

                    processMessage(message);
                } catch (Exception ioe) {
                    System.out.println(ioe);
                    isOpen = false;
                }
            }
        }

        try{
            handleShutdown(null);
        } catch (Exception e){
            System.out.println(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void handleShutdown(ECSMessage message) throws Exception{
        if (serverSocket == null) return;
        ECSNode nextNode = this.ecs.removeNode(node);
        if (message != null){
            HashMap<String, String> kvPairs = (HashMap<String, String>) message.getParameter("KV_PAIRS");
            if(!kvPairs.isEmpty() && nextNode != null){
                logger.info("Transferring " + kvPairs.size() + " key-value pairs from " + this.node.getNodeName() + " to " + nextNode.getNodeName());
                logger.info("kvPairs: " + kvPairs.toString());
                
                if (node != null && nextNode != null && kvPairs != null && kvPairs.size() > 0){
                    messageService.sendECSMessage(nextNode.getServerSocket(), nextNode.getObjectOutputStream(), ECSMessageType.RECEIVE, "FROM_NODE", null, "KV_PAIRS", kvPairs);
                }
            }
        }

        ECS.connections.remove(this);

        try {
            if (serverSocket != null)
                serverSocket.close();

            logger.info("Connection closed for " + serverSocket.getInetAddress().getHostName());
            this.serverSocket = null;
        } catch (IOException e) {
            logger.error("Error! closing connection", e);
        }
    }


    private void handleInit(ECSMessage message) throws Exception{
        String serverName = (String) message.getParameter("SERVER_NAME");
        logger.info("ECS connected to KVServer via " +  serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort());

        // split serverName by : to get the server address and port
        String[] serverInfo = serverName.split(":");
        String serverAddress = serverInfo[0];
        int serverPort = Integer.parseInt(serverInfo[1]);

        this.node = new ECSNode(serverName, serverAddress, serverPort, serverSocket, outStream);

        ECSNode oldNode = this.ecs.addNode(this.node);

        // oldNode is null if newNode is the only node in the hashring
        if (oldNode != null) {
            messageService.sendECSMessage(oldNode.getServerSocket(), oldNode.getObjectOutputStream(),ECSMessageType.TRANSFER_FROM, "TO_NODE", this.node);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleTransferTo(ECSMessage message) throws Exception{
        System.out.println("TRANSFER_TO Command");
        ECSNode toNode = (ECSNode) message.getParameter("TO_NODE");
        HashMap<String, String> kvPairs = (HashMap<String, String>) message.getParameter("KV_PAIRS");

        if(kvPairs != null && kvPairs.size() > 0){
            logger.info("Transferring " + kvPairs.size() + " key-value pairs from " + this.node.getNodeName() + " to " + this.node.getNodeName());
            logger.info("kvPairs: " + kvPairs.toString());
            Socket toNodeSocket = this.ecs.nodes.get(toNode.getNodeName()).getServerSocket();
            ObjectOutputStream out = this.ecs.nodes.get(toNode.getNodeName()).getObjectOutputStream();

            messageService.sendECSMessage(toNodeSocket, out, ECSMessageType.RECEIVE, "FROM_NODE", node, "KV_PAIRS", kvPairs);
        }
    }

    private void handleTransferComplete(ECSMessage message) throws Exception{
        System.out.println("TRANSFER_COMPLETE Command");
        ECSNode pingNode = (ECSNode) message.getParameter("PING_NODE");
        Socket pingNodeSocket = this.ecs.nodes.get(pingNode.getNodeName()).getServerSocket();
        ObjectOutputStream out = this.ecs.nodes.get(pingNode.getNodeName()).getObjectOutputStream();

        messageService.sendECSMessage(pingNodeSocket, out, ECSMessageType.TRANSFER_COMPLETE, "PING_NODE", pingNode);
    }

    private void processMessage(ECSMessage message){
        try{
            switch (message.getType()){
                case INIT:
                    handleInit(message);
                    break;
                case TRANSFER_TO:
                    handleTransferTo(message);
                    break;
                case SHUTDOWN:
                    handleShutdown(message);
                    break;
                case TRANSFER_COMPLETE:
                    handleTransferComplete(message);
                    break;
                default:
                    System.out.println("Unrecognized Command in ECS" + message);
            }
        } catch (Exception e){
            System.out.println(e);
        }
    }
}
