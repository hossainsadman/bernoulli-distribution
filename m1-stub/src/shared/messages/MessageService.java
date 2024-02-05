package shared.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger; // import Logger

public class MessageService {
  private static Logger logger = Logger.getRootLogger();

  private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
  
  /**
   * Send message (that implements MessageInterface) to the specified socket
   * 
   * @param socket
   * @param msg
   * @throws IOException
   */
  public void sendBasicKVMessage(Socket socket, BasicKVMessage msg) throws IOException {
    this.logger.info("Sending Message on socket");
    OutputStream output = socket.getOutputStream();
    byte[] msgBytes = msg.getMsgBytes();
    output.write(msgBytes, 0, msgBytes.length);
    output.flush();
  }
  
  /**
   * Receive message from socket and construct and return it as a BasicKVMessage type.
   * 
   * @param socket
   * @return
   * @throws IOException
   */
  public BasicKVMessage receiveBasicKVMessage(Socket socket) throws IOException {
    byte[] receivedMessageBytes = this.receiveMessage(socket);
    this.logger.info("Received message on socket");
    BasicKVMessage msg = new BasicKVMessage(receivedMessageBytes);
    return msg;
  }

  private byte[] receiveMessage(Socket socket) throws IOException {
    InputStream input = socket.getInputStream();
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

    /* read first char from stream */
    byte prevRead = 0;
		byte read = (byte) input.read();	
		boolean reading = true;

		while(!(read == 10 && prevRead == 13) && reading) {/* CR, LF, error */
          /* if buffer filled, copy to msg array */
      if (index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
        index = 0;
			} 

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
      prevRead = read;
      read = (byte) input.read();
		}

    if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
    } else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
      System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

    msgBytes = tmp;
    return msgBytes;
  }
}
