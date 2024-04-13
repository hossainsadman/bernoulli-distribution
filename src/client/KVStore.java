package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import org.apache.log4j.Logger; // import Logger
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.CommunicationService;
import ecs.ECSHashRing;
import ecs.ECSNode;

public class KVStore implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     * 
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    private static Logger logger = Logger.getRootLogger();
    private ObjectMapper om = new ObjectMapper();
    private String serverAddress;
    private int serverPort;
    private CommunicationService communicationService;
    private ECSHashRing metaData;

    private static final int MAX_KEY_BYTES = 20;
    private static final int MAX_VALUE_BYTES = 120 * 1024; // 120 kB

    public int maxRetries = 10;
    public boolean testing = false;

    public KVStore(String address, int port) {
        this.serverAddress = address;
        this.serverPort = port;
        this.metaData = null;
        this.om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    @Override
    public void connect() throws Exception {
        this.communicationService = new CommunicationService("KVStore", this.serverAddress, this.serverPort);
        this.communicationService.connect();
    }

    @Override
    public void disconnect() {
        this.communicationService.disconnect();
    }

    public void reconnect(String server, int port) throws Exception {
        disconnect();
        this.serverAddress = server;
        this.serverPort = port;
        connect();
    }

    private void updateMetadata(BasicKVMessage message) {
        try {
            this.metaData = this.om.readValue(message.getKey(), ECSHashRing.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setTesting(boolean testing) {
        this.testing = testing;
    }

    public ECSNode getResponsibleNode(String key) {
        return this.metaData.getNodeForKey(key);
    }

    private BasicKVMessage sendMessageToServer(BasicKVMessage message) throws Exception {
        BasicKVMessage response = null;
        int retryCount = 0;

        do {
            if(this.metaData != null && !this.testing){
                System.out.println("[KVStore] :: Reconnecting to server");
                ECSNode tryServer = this.metaData.getNodeForKey(message.getKey());
                reconnect(tryServer.getNodeHost(), tryServer.getNodePort());
            }
            
            this.communicationService.sendMessage(message);
            response = this.communicationService.receiveMessage();

            if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE){
                System.out.println("[KVStore]: SERVER_NOT_RESPONSIBLE");
                updateMetadata(response);
                retryCount++;
            } else {
                // Request sent to correct server
                break;
            }
        } while (retryCount <= this.maxRetries);

        return response;
    }

    @Override
    public BasicKVMessage put(String key, String value) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, value);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.PUT, key, value);

        return this.sendMessageToServer(message);
    }

    @Override
    public BasicKVMessage get(String key) throws Exception {
        BasicKVMessage invalidParmetersError = this.validateKeyValuePair(key, null);
        if (invalidParmetersError != null)
            return invalidParmetersError;

        BasicKVMessage message = new BasicKVMessage(StatusType.GET, key, null);

        return this.sendMessageToServer(message);
    }

    public BasicKVMessage getAllKeys() throws Exception {
        BasicKVMessage message = new BasicKVMessage(StatusType.GET_ALL_KEYS, null, null);
        return this.sendMessageToServer(message);
    }

    public BasicKVMessage keyrange() throws Exception {
        BasicKVMessage message = new BasicKVMessage(StatusType.KEYRANGE, null, null);
        if(this.metaData != null){
            ECSNode server = this.metaData.getFirstNode();
            reconnect(server.getNodeHost(), server.getNodePort());
        }

        this.communicationService.sendMessage(message);
        return this.communicationService.receiveMessage();
    }

    public BasicKVMessage keyrangeRead() throws Exception {
        BasicKVMessage message = new BasicKVMessage(StatusType.KEYRANGE_READ, null, null);
        if(this.metaData != null){
            ECSNode server = this.metaData.getFirstNode();
            reconnect(server.getNodeHost(), server.getNodePort());
        }

        this.communicationService.sendMessage(message);
        return this.communicationService.receiveMessage();
    }

    private BasicKVMessage validateKeyValuePair(String key, String value) {
        if (key.length() > MAX_KEY_BYTES || key.isEmpty())
            return new BasicKVMessage(StatusType.INVALID_KEY, "Key must be non-empty and less than or equal to 20 bytes",
                    null);

        if (value != null && value.length() > MAX_VALUE_BYTES)
            return new BasicKVMessage(StatusType.INVALID_VALUE, "Value must be less than or equal to 120 kilobytes", null);

        return null;
    }

	public ECSHashRing getMetaData() {
		return this.metaData;
	}

    public BasicKVMessage sqlcreate(String key, String value) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, value);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.SQLCREATE, key, value);

        return this.sendMessageToServer(message);
    }

    public BasicKVMessage sqlselect(String key) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, null);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.SQLSELECT, key, null);

        return this.sendMessageToServer(message);
    }

    public BasicKVMessage sqldrop(String key) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, null);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.SQLDROP, key, null);

        return this.sendMessageToServer(message);
    }

    public BasicKVMessage sqlinsert(String key, String value) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, value);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.SQLINSERT, key, value);

        return this.sendMessageToServer(message);
    }

    public BasicKVMessage sqlupdate(String key, String value) throws Exception {
        BasicKVMessage invalidParametersError = this.validateKeyValuePair(key, value);
        if (invalidParametersError != null)
            return invalidParametersError;
            
        BasicKVMessage message = new BasicKVMessage(StatusType.SQLUPDATE, key, value);

        return this.sendMessageToServer(message);
    }
}