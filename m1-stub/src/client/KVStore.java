package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import org.apache.log4j.Logger; // import Logger

import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.CommunicationService;

public class KVStore implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     * 
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    private CommunicationService communicationService;

    private static final int MAX_KEY_BYTES = 20;
    private static final int MAX_VALUE_BYTES = 120 * 1024; // 120 kB

    public KVStore(String address, int port) {
        this.communicationService = new CommunicationService("KVStore", address, port);
    }

    @Override
    public void connect() throws Exception {
        this.communicationService.connect();
    }

    @Override
    public void disconnect() {
        this.communicationService.disconnect();
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        BasicKVMessage invalidParmetersError = this.validateKeyValuePair(key, value);
        if (invalidParmetersError != null)
            return invalidParmetersError;

        BasicKVMessage message = new BasicKVMessage(StatusType.PUT, key, value);
        this.communicationService.sendMessage(message);
        return this.communicationService.receiveMessage();
    }

    @Override
    public BasicKVMessage get(String key) throws Exception {
        BasicKVMessage invalidParmetersError = this.validateKeyValuePair(key, null);
        if (invalidParmetersError != null)
            return invalidParmetersError;

        BasicKVMessage message = new BasicKVMessage(StatusType.GET, key, null);
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
}
