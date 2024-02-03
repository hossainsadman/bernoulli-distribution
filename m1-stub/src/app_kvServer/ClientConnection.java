package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;
import app_kvServer.KVServer.*;
import client.KVStore;
import shared.messages.KVMessage;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * 
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            sendMessage(new TextMessage(
                    "Connection to MSRG Echo server established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort()));

            while (isOpen) {
                try {
                    TextMessage recv = receiveMessage();
                    processMessage(recv);

                    /*
                     * connection either terminated by the client or lost due to
                     * network problems
                     */
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {
            closeConnection();
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     * 
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() + "'");
    }

    public void closeConnection() {
        try {
            if (clientSocket != null)
                clientSocket.close(); 
            if (input != null) 
                input.close(); 
            if (output != null) 
                output.close(); 
            logger.info("Connection closed for " + clientSocket.getInetAddress().getHostName());
        } catch (IOException e) {
            logger.error("Error! closing connection", e);
        }
    }

    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }
    
    // Process received messages, and send it back to the client
    private void processMessage(TextMessage recv) throws IOException {
        String[] tokens = recv.getMsg().trim().split("\\s+", 3);
        TextMessage response;

        if (tokens[0].equalsIgnoreCase("put") && tokens.length == 3 && tokens[1] != null && tokens[2] != null) { // PUT (WILL BE MODIFIED)
            // String result = KVServer.putKV(tokens[1], tokens[2]);

            // tuple successfully inserted
            // send acknowledgement to client: PUT_SUCCESS <key> <value>

            // tuple successfully updated
            // send acknowledgement to client: PUT_UPDATE <key> <value>

            // unable to insert tuple
            // send error message to client:
            // PUT_ERROR <key> <value>
            // Customize this response based on your application's specific needs
            // response = "PUT_SUCCESS " + tokens[1] + " " + tokens[2];
            return;
        } else if (tokens[0].equalsIgnoreCase("get") && tokens.length == 2 && tokens[1] != null) { // GET (WILL BE MODIFIED)
            // String value = KVServer.getKV(tokens[1]);

            // if (value == null) {
            //     // send error message to client:
            //     // GET_ERROR <key>
            //     sendMessage(null);
            // } else {
            //     // GET_SUCCESS <key> <value> to client.
            //     sendMessage(null);
            // }
        } else { // PUT (WILL BE MODIFIED)
            // KVMessage response = FAILED <error description>
            sendMessage(null);
            return;
        }
    }
}