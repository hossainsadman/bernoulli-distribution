package shared;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageService;

import org.apache.log4j.Logger;

public class CommunicationService {
  private static Logger logger = Logger.getRootLogger();
  private int port;
  private String address;
  private Socket socket;
  private String origin;
  private MessageService messageService = new MessageService();

  public CommunicationService(String origin, String address, int port) {
    this.address = address;
    this.port = port;
    this.origin = origin;
  }

  public void connect() throws IOException {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException(generateLogMessage("Invalid port number: " + port));
    }

    try {
      socket = new Socket(address, port);
      logger.info(generateLogMessage("Connected to server: " + address + ":" + port));
    } catch (UnknownHostException e) {
      throw new UnknownHostException(generateLogMessage("Unknown host: " + address));
    } catch (IOException e) {
      throw new IOException(generateLogMessage("Unable to connect to the server"), e);
    }
  }

  public void disconnect() {
    try {
      if (socket != null && !socket.isClosed()) {
        socket.close();
        logger.info(generateLogMessage("Disconnected from server."));
      }
    } catch (IOException e) {
      logger.error(generateLogMessage("Error when disconnecting from server"), e);
    }
  }

  public void sendMessage(BasicKVMessage msg) throws IOException {
    if (socket == null || socket.isClosed()) {
      throw new IOException(generateLogMessage("Socket is not connected"));
    }

    messageService.sendBasicKVMessage(socket, msg);
  }

  public BasicKVMessage receiveMessage() throws IOException {
    if (socket == null || socket.isClosed()) {
      throw new IOException(generateLogMessage("Socket is not connected"));
    }

    return messageService.receiveBasicKVMessage(socket);
  }

  private String generateLogMessage(String message){
    return "[" + origin + "]" + message;
  }
}
