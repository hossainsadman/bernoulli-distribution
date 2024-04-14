package app_kvServer;

import java.io.IOException;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;
import shared.CommunicationService;

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
            } catch (Exception e) {
                logger.error("Error! while processing message", e);
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
    private void processMessage(BasicKVMessage recv) throws IOException, Exception {
        BasicKVMessage res;
        StatusType recvStatus = recv.getStatus();
        String recvKey = recv.getKey();
        String recvVal = recv.getValue();
        Boolean recvLocolProtocol = recv.getLocalProtocol();

        if (recvStatus == StatusType.GET_ALL_KEYS){
            res = new BasicKVMessage(StatusType.GET_ALL_KEYS, this.server.getAllKvPairsResponsibleFor().toString(), null);
        }
        else if (recvStatus == StatusType.KEYRANGE_READ){
            res = new BasicKVMessage(StatusType.KEYRANGE_READ_SUCCESS, this.server.getHashRing().keyrangeRead(), null);
        }
        else if (recvStatus == StatusType.KEYRANGE){
            res = new BasicKVMessage(StatusType.KEYRANGE_SUCCESS, this.server.getHashRing().toString(), null);
        } 
        else if (recvStatus == StatusType.REPLICATE){
            System.out.println("[KVServer] Received REPLICATE command (" + recvKey + "," + recvVal + ")");

            try {
                server.putKV(recvKey, recvVal);
                res = new BasicKVMessage(StatusType.REPLICATE_SUCCESS, recvKey, recvVal);
            } catch (Exception e) { 
                if (recvVal.equals("null"))
                    res = new BasicKVMessage(StatusType.DELETE_ERROR, recvKey, recvVal);
                else
                    res = new BasicKVMessage(StatusType.PUT_ERROR, recvKey, recvVal);
            }

        } 
        else if (recvStatus == StatusType.PUT && recvKey != null && recvVal != null) { // PUT
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
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

                    if (putStatus != StatusType.SERVER_WRITE_LOCK){
                        if (this.server.replicate(recvKey, recvVal)){
                            this.logger.info("Replication success");
                        } else {
                            this.logger.info("Replication failure");
                        }
                    }

                } catch (Exception e) {
                    if (recvVal.equals("null"))
                        res = new BasicKVMessage(StatusType.DELETE_ERROR, recvKey, recvVal);
                    else
                        res = new BasicKVMessage(StatusType.PUT_ERROR, recvKey, recvVal);
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.PUT && recvVal == null) {
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
                res = new BasicKVMessage(StatusType.PUT_ERROR, recvKey, recvVal);
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.GET && recvKey != null) { // GET
            if(this.server.isCoordinatorOrReplicator(KVServer.escape(recvKey))){
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
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.SQLCREATE && recvKey != null && recvVal != null) {
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
                /*
                * tuple successfully inserted, send acknowledgement to client: PUT_SUCCESS
                * <key> <value>
                * tuple successfully updated, send acknowledgement to client: PUT_UPDATE <key>
                * <value>
                * unable to insert tuple, send error message to client: PUT_ERROR <key> <value>
                */

                try {
                    StatusType sqlCreateStatus;
                    sqlCreateStatus = server.sqlCreate(recvKey, recvVal, false);
                    res = new BasicKVMessage(sqlCreateStatus, recvKey, recvVal);

                    if (sqlCreateStatus != StatusType.SERVER_WRITE_LOCK){
                        if (this.server.replicateSQLCommand(recvKey, recvVal, StatusType.SQLCREATE_REPLICATE)){
                            this.logger.info("SQLCREATE_REPLICATE Replication success");
                        } else {
                            this.logger.info("SQLCREATE_REPLICATE Replication failure");
                        }
                    }

                } catch (Exception e) {
                    res = new BasicKVMessage(StatusType.SQLCREATE_ERROR, recvKey, recvVal);
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.SQLSELECT && recvKey != null) {
            if(this.server.isCoordinatorOrReplicator(KVServer.escape(recvKey))){
                try {
                    boolean testing = false;
                    if (recvVal != null) {
                        testing = true;
                    }
                    String value = server.sqlSelect(recvKey, testing);

                    if (value == null) // table not found, send error message to client: SQLSELECT_ERROR <tablename>
                        res = new BasicKVMessage(StatusType.SQLSELECT_ERROR, recvKey, null);
                    else // table found: SQLSELECT_SUCCESS <tablename> <tablecontents> to client.
                        res = new BasicKVMessage(StatusType.SQLSELECT_SUCCESS, recvKey, value);

                } catch (Exception e) {
                    res = new BasicKVMessage(StatusType.SQLSELECT_ERROR, recvKey, recvVal);
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.SQLDROP && recvKey != null) {
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
                try {
                    StatusType sqlDropStatus;
                    sqlDropStatus = server.sqlDrop(recvKey, false);
                    res = new BasicKVMessage(sqlDropStatus, recvKey, null);

                    if (sqlDropStatus != StatusType.SERVER_WRITE_LOCK){
                        if (this.server.replicateSQLCommand(recvKey, null, StatusType.SQLDROP_REPLICATE)){
                            this.logger.info("SQLDROP_REPLICATE Replication success");
                        } else {
                            this.logger.info("SQLDROP_REPLICATE Replication failure");
                        }
                    }

                } catch (Exception e) {
                    res = new BasicKVMessage(StatusType.SQLDROP_ERROR, recvKey, null);
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }
        } 
        else if (recvStatus == StatusType.SQLINSERT && recvKey != null && recvVal != null) {
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
                /*
                * tuple successfully inserted, send acknowledgement to client: PUT_SUCCESS
                * <key> <value>
                * tuple successfully updated, send acknowledgement to client: PUT_UPDATE <key>
                * <value>
                * unable to insert tuple, send error message to client: PUT_ERROR <key> <value>
                */

                try {
                    StatusType sqlInsertStatus;
                    sqlInsertStatus = server.sqlInsert(recvKey, recvVal, false);
                    res = new BasicKVMessage(sqlInsertStatus, recvKey, recvVal);

                    if (sqlInsertStatus != StatusType.SERVER_WRITE_LOCK){
                        if (this.server.replicateSQLCommand(recvKey, recvVal, StatusType.SQLINSERT_REPLICATE)){
                            this.logger.info("SQLINSERT_REPLICATE Replication success");
                        } else {
                            this.logger.info("SQLINSERT_REPLICATE Replication failure");
                        }
                    }

                } catch (Exception e) {
                    res = new BasicKVMessage(StatusType.SQLINSERT_ERROR, recvKey, recvVal);
                    this.logger.error("Error occurred during SQLINSERT: " + e.getMessage());
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.SQLUPDATE && recvKey != null && recvVal != null) {
            if(this.server.isCoordinator(KVServer.escape(recvKey))){
                /*
                * tuple successfully inserted, send acknowledgement to client: PUT_SUCCESS
                * <key> <value>
                * tuple successfully updated, send acknowledgement to client: PUT_UPDATE <key>
                * <value>
                * unable to insert tuple, send error message to client: PUT_ERROR <key> <value>
                */

                try {
                    StatusType sqlUpdateStatus;
                    sqlUpdateStatus = server.sqlUpdate(recvKey, recvVal, false);
                    res = new BasicKVMessage(sqlUpdateStatus, recvKey, recvVal);

                    if (sqlUpdateStatus != StatusType.SERVER_WRITE_LOCK){
                        if (this.server.replicateSQLCommand(recvKey, recvVal, StatusType.SQLUPDATE_REPLICATE)){
                            this.logger.info("SQLUPDATE_REPLICATE Replication success");
                        } else {
                            this.logger.info("SQLUPDATE_REPLICATE Replication failure");
                        }
                    }

                } catch (Exception e) {
                    res = new BasicKVMessage(StatusType.SQLUPDATE_ERROR, recvKey, recvVal);
                    this.logger.error("Error occurred during SQLUPDATE: " + e.getMessage());
                }
            } else {
                if(recvLocolProtocol){
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, this.om.writeValueAsString(this.server.getHashRing()), null);
                } else{
                    res = new BasicKVMessage(StatusType.SERVER_NOT_RESPONSIBLE, null, null);
                }
            }

        } 
        else if (recvStatus == StatusType.SQLCREATE_REPLICATE && recvKey != null && recvVal != null) {
            System.out.println("[KVServer] Received SQLCREATE_REPLICATE command (" + recvKey + "," + recvVal + ")");

            try {
                StatusType sqlCreateStatus;
                sqlCreateStatus = server.sqlCreate(recvKey, recvVal, false);
                res = new BasicKVMessage(sqlCreateStatus, recvKey, recvVal);
            } catch (Exception e) {
                res = new BasicKVMessage(StatusType.SQLCREATE_REPLICATE_ERROR, recvKey, recvVal);
                this.logger.error("Error occurred during SQLCREATE_REPLICATE_ERROR: " + e.getMessage());
            }

        } 
        else if (recvStatus == StatusType.SQLDROP_REPLICATE && recvKey != null) {
            System.out.println("[KVServer] Received SQLDROP_REPLICATE command (" + recvKey + ")");

            try {
                StatusType sqlDropStatus;
                sqlDropStatus = server.sqlDrop(recvKey, false);
                res = new BasicKVMessage(sqlDropStatus, recvKey, null);
            } catch (Exception e) {
                res = new BasicKVMessage(StatusType.SQLDROP_REPLICATE_ERROR, recvKey, null);
                this.logger.error("Error occurred during SQLDROP_REPLICATE_ERROR: " + e.getMessage());
            }

        } 
        else if (recvStatus == StatusType.SQLINSERT_REPLICATE && recvKey != null && recvVal != null) {
            System.out.println("[KVServer] Received SQLINSERT_REPLICATE command (" + recvKey + "," + recvVal + ")");

            try {
                StatusType sqlInsertStatus;
                sqlInsertStatus = server.sqlInsert(recvKey, recvVal, false);
                res = new BasicKVMessage(sqlInsertStatus, recvKey, recvVal);
            } catch (Exception e) {
                res = new BasicKVMessage(StatusType.SQLINSERT_REPLICATE_ERROR, recvKey, recvVal);
                this.logger.error("Error occurred during SQLINSERT_REPLICATE: " + e.getMessage());
            }
            
        } 
        else if (recvStatus == StatusType.SQLUPDATE_REPLICATE && recvKey != null && recvVal != null) {
            System.out.println("[KVServer] Received SQLUPDATE_REPLICATE command (" + recvKey + "," + recvVal + ")");

            try {
                StatusType sqlUpdateStatus;
                sqlUpdateStatus = server.sqlUpdate(recvKey, recvVal, false);
                res = new BasicKVMessage(sqlUpdateStatus, recvKey, recvVal);
            } catch (Exception e) {
                res = new BasicKVMessage(StatusType.SQLUPDATE_REPLICATE_ERROR, recvKey, recvVal);
                this.logger.error("Error occurred during SQLUPDATE_REPLICATE: " + e.getMessage());
            }
            
        }
        else if (recvStatus == StatusType.INVALID_KEY || recvStatus == StatusType.INVALID_VALUE) { 
            // message size exceeded
            res = new BasicKVMessage(StatusType.FAILED, recvKey, null);
        } 
        else { // Message format unknown
            res = new BasicKVMessage(StatusType.FAILED, "Message format is unknown.", null);
        }

        res.setLocalProtocl(recvLocolProtocol);
        comm.sendMessage(res);
    }
}