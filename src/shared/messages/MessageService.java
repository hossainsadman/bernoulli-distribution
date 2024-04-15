package shared.messages;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import shared.ConsoleColors;

import org.apache.log4j.Logger; // import Logger

import shared.messages.ECSMessage.ECSMessageType;

public class MessageService {
    private static Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    public void sendECSMessage(Socket socket, ObjectOutputStream out, ECSMessageType messageType, Object... params)
            throws Exception {
        if (params.length % 2 != 0) {
            throw new IllegalArgumentException("Parameters should be in key-value pairs");
        }
        HashMap<String, Serializable> parameters = new HashMap<>();
        for (int i = 0; i < params.length; i += 2) {
            if (!(params[i] instanceof String)) {
                throw new IllegalArgumentException("Key must be a string");
            }
            parameters.put((String) params[i], (Serializable) params[i + 1]);
        }

        // Verify required parameters for the message type
        for (String requiredParam : messageType.getRequiredParameters()) {
            if (!parameters.containsKey(requiredParam)) {
                throw new IllegalArgumentException("Missing required parameter: " + requiredParam);
            }
        }

        ECSMessage message = new ECSMessage(messageType, parameters);

        writeObjectToSocket(socket, out, message);
    }

    public ECSMessage receiveECSMessage(Socket socket, ObjectInputStream in) throws Exception {
        Object receivedObject = readObjectFromSocket(socket, in);
        if (receivedObject == null) {
            // Connection has been closed, handle gracefully
            this.logger.info(ConsoleColors.RED_UNDERLINED + "Socket connection closed, stopping listener." + ConsoleColors.RESET);
            return null;
        }
        // If receivedObject is not null, cast to ECSMessage and process further
        return (ECSMessage) receivedObject;
    }

    /**
     * Send message (that implements MessageInterface) to the specified socket
     * 
     * @param socket
     * @param msg
     * @throws IOException
     */
    public void sendBasicKVMessage(Socket socket, BasicKVMessage msg) throws IOException {
        OutputStream output = socket.getOutputStream();
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    /**
     * Receive message from socket and construct and return it as a BasicKVMessage
     * type.
     * 
     * @param socket
     * @return
     * @throws IOException
     */
    public BasicKVMessage receiveBasicKVMessage(Socket socket) throws IOException {
        byte[] receivedMessageBytes = this.receiveMessage(socket);
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

        while (!(read == 10 && prevRead == 13) && reading) {/* CR, LF, error */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
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
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
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

    private Object readObjectFromSocket(Socket socket, ObjectInputStream in) {
        Object obj = null;
        try {
            obj = in.readObject();
        } catch (EOFException e) {
            System.out.println(ConsoleColors.RED_UNDERLINED + "[MessageService.readObjectFromSocket] Connection has been closed by the other side." + ConsoleColors.RESET);
        } catch (SocketException e) {
            System.out.println(
                    ConsoleColors.RED_UNDERLINED + "[MessageService.readObjectFromSocket] java.net.SocketException: Socket most likely closed from other side" + ConsoleColors.RESET);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

    private void writeObjectToSocket(Socket socket, ObjectOutputStream out, Object obj) {
        try {
            out.writeObject(obj);
            out.reset();
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
