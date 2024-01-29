package client;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import org.apache.log4j.Logger; // import Logger

import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageService;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	
	private String address;
	private int port;
	private Socket socket;
  private static Logger logger = Logger.getRootLogger();
    private MessageService messageService = new MessageService();
	
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		if (port < 0 || port > 65535) {
			throw new IllegalArgumentException("Invalid port number: " + port);
		}
		try {
			socket = new Socket(address, port);
			logger.info("Connected to server: " + address + ":" + port);
		} catch (UnknownHostException e) {
			throw new UnknownHostException("Unknown host: " + address);
		} catch (IOException e) {
			throw new IOException("Unable to connect to the server", e);
		}
	}

	@Override
	public void disconnect() {
		try {
			if (socket != null && !socket.isClosed()) {
				socket.close();
				logger.info("Disconnected from server.");
			}
		} catch (IOException e) {
			throw new Exception("Error when disconnecting from server");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
    // TODO: create BasicKVMessage here
    BasicKVMessage message = new BasicKVMessage(StatusType.PUT, key, value);
    this.messageService.sendMessage(socket, message);
    return this.messageService.receiveBasicKVMessage(socket);
	}

	@Override
	public BasicKVMessage get(String key) throws Exception {
    // TODO: create BasicKVMessage here
    BasicKVMessage message = new BasicKVMessage(StatusType.GET, key, null);
    this.messageService.sendMessage(socket, message);
    return this.messageService.receiveBasicKVMessage(socket);
  }
}
