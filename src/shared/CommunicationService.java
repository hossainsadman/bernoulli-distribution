package shared;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageService;
import shared.ConsoleColors;

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

  public CommunicationService(Socket socket) {
    this.socket = socket;
  }

  public void connect() throws IOException {
    if (port < 1024 || port > 65535) {
      logger.error(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Invalid port number: " + port));
      throw new IllegalArgumentException(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Invalid port number: " + port));
    }

    try {
      socket = new Socket(address, port);
      System.out.println(generateLogMessage(ConsoleColors.GREEN_UNDERLINED + "Connected to server: " + address + ":" + port + ConsoleColors.RESET));
    } catch (UnknownHostException e) {
      System.out.println(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Unknown host: " + address));
      throw new UnknownHostException(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Unknown host: " + address));
    } catch (IOException e) {
      System.out.println("Unable to connect to " + address + ":" + port);
      logger.error(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Unable to connect to the server"));
      throw new IOException(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Unable to connect to the server"), e);
    }
  }

  public void disconnect() {
    try {
      if (socket != null && !socket.isClosed()) {
        socket.close();
        System.out.println(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Disconnected from server."));
      }
    } catch (IOException e) {
      System.out.println("Unable to disconnect from " + address + ":" + port);
      logger.error(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Error when disconnecting from server"), e);
    }
  }

  public void sendMessage(BasicKVMessage msg) throws IOException {
    if (socket == null || socket.isClosed()) {
      logger.error(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Socket is not connected"));
      throw new IOException(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Socket is not connected"));
    }

    messageService.sendBasicKVMessage(socket, msg);
  }

  public BasicKVMessage receiveMessage() throws IOException {
    if (socket == null || socket.isClosed()) {
      logger.error(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Socket is not connected"));
      throw new IOException(generateLogMessage(ConsoleColors.RED_UNDERLINED + "Socket is not connected"));
    }

    return messageService.receiveBasicKVMessage(socket);
  }

  private String generateLogMessage(String message){
    return "[" + origin + "] " + message + ConsoleColors.RESET;
  }
}
