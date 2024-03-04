package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.*;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.KVServer.*;
import client.KVStore;
import ecs.ECSNode;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.CommunicationService;
import shared.MD5;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();
    private ObjectMapper om = new ObjectMapper();
    private KVServer server;
    private CommunicationService comm;
    private Socket clientSocket;
    private boolean isOpen;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * 
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(KVServer server, Socket clientSocket) {
        this.server = server;
        this.comm = new CommunicationService(clientSocket);
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        while (isOpen) {
            try {
                BasicKVMessage recv = comm.receiveMessage();
                processMessage(recv);
            } catch (IOException ioe) {
                isOpen = false;
            }
        }
        close();
    }

    /**
     * Closes the client connection.
     */
    public void close() {
        try {
            if (clientSocket != null)
                clientSocket.close();
            if (comm != null)
                comm.disconnect();

            logger.info("Connection closed for " + clientSocket.getInetAddress().getHostName());
        } catch (IOException e) {
            logger.error("Error! closing connection", e);
        }
    }

    private boolean checkKeyInRange(String key) {
        return this.server.getMetadata().isKeyInRange(key);
    }

    /**
     * Processes received messages, and send it back to the client.
     */
    private void processMessage(BasicKVMessage recv) throws IOException {
        this.logger.info("Handling command");
        BasicKVMessage res;
        StatusType recvStatus = recv.getStatus();
        String recvKey = recv.getKey();
        String recvVal = recv.getValue();
        Boolean recvLocolProtocol = recv.getLocalProtocol();

        if (recvStatus == StatusType.KEYRANGE){
            res = new BasicKVMessage(StatusType.KEYRANGE_SUCCESS, this.server.getHashRing().toString(), null);
        } else if (recvStatus == StatusType.PUT && recvKey != null && recvVal != null) { // PUT
            if(checkKeyInRange(recvKey)){
                /*
                * tuple successfully inserted, send acknowledgement to client: PUT_SUCCESS
                * <key> <value>
                * tuple successfully updated, send acknowledgement to client: PUT_UPDATE <key>
                * <value>
                * unable to insert tuple, send error message to client: PUT_ERROR <key> <value>
                */

                try {
                    StatusType putStatus;
                    putStatus = server.putKV(recvKey, recvVal);
                    res = new BasicKVMessage(putStatus, recvKey, recvVal);
                } catch (Exception e) {
                    if (recvVal.equals("null"))
                        res = new BasicKVMessage(StatusType.DELETE_ERROR, recvKey, recvVal);
                    else
                        res = new BasicKVMessage(StatusType.PUT_ERROR, recvKey, recvVal);
                }
            } else {
                res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
            }

        } else if (recvStatus == StatusType.PUT && recvVal == null) {
            if(checkKeyInRange(recvKey)){
                res = new BasicKVMessage(StatusType.PUT_ERROR, recvKey, recvVal);
            } else {
                res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
            }

        } else if (recvStatus == StatusType.GET && recvKey != null) { // GET
            if(checkKeyInRange(recvKey)){
                try {
                    String value = server.getKV(recvKey);
    
                    if (value == null) // tuple not found, send error message to client: GET_ERROR <key>
                        res = new BasicKVMessage(StatusType.GET_ERROR, recvKey, null);
                    else // tuple found: GET_SUCCESS <key> <value> to client.
                        res = new BasicKVMessage(StatusType.GET_SUCCESS, recvKey, value);
                } catch (Exception e) { // Something is wrong.
                    res = new BasicKVMessage(StatusType.GET_ERROR, recvKey, null);
                }
            } else {
                res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
            }

        } else if (recvStatus == StatusType.INVALID_KEY || recvStatus == StatusType.INVALID_VALUE) { 
            // message size exceeded
            res = new BasicKVMessage(StatusType.FAILED, recvKey, null);
        } else { // Message format unknown
            res = new BasicKVMessage(StatusType.FAILED, "Message format is unknown.", null);
        }
        res.setLocalProtocl(recvLocolProtocol);
        comm.sendMessage(res);
    }
}