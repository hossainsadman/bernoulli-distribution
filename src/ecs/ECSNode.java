package ecs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.*;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger; // import Logger

import app_kvServer.ClientConnection;
import shared.CommunicationService;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;
import shared.MD5;
import com.fasterxml.jackson.annotation.JsonIgnore;
import shared.ConsoleColors;

public class ECSNode implements IECSNode, Serializable {
    private String name;
    private String host;
    private Integer port;
    private BigInteger identifier;
    private BigInteger hashStartRange;
    private BigInteger hashEndRange;
    private transient String cacheStrategy = "None";
    private transient int cacheSize = 0;
    private transient Socket serverSocket = null;
    private transient CommunicationService comm;

    @JsonIgnore
    private transient ObjectOutputStream ecsOutStream;

    private static Logger logger = Logger.getRootLogger();

    public static final BigInteger RING_START = BigInteger.ZERO;
    public static final BigInteger RING_END = new BigInteger(String.valueOf('F').repeat(32), 16);

    ECSNode() { }

    ECSNode(BigInteger start, BigInteger end) {
        this.host = host;
        this.port = port;
        this.identifier = MD5.getHash(host + ":" + port);
        this.hashStartRange = start;
        this.hashEndRange = end;
    }

    ECSNode(String host, Integer port, BigInteger start, BigInteger end) {
        this.host = host;
        this.port = port;
        this.identifier = MD5.getHash(host + ":" + port);
        this.hashStartRange = start;
        this.hashEndRange = end;
    }

    public ECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.identifier = MD5.getHash(host + ":" + port);
    }

    public ECSNode(String name, String host, Integer port, Socket serverSocket, ObjectOutputStream outputStream) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.identifier = MD5.getHash(host + ":" + port);
        this.serverSocket = serverSocket;
        this.comm = new CommunicationService(serverSocket);
        this.ecsOutStream = outputStream;
    }

    public void closeConnection() throws IOException {
        try {
            if (serverSocket != null)
                serverSocket.close();
            if (comm != null)
                comm.disconnect();

            logger.info(ConsoleColors.RED_UNDERLINED + "Connection closed for " + serverSocket.getInetAddress().getHostName() + ConsoleColors.RESET);
        } catch (IOException e) {
            logger.error(ConsoleColors.RED_UNDERLINED + "Error! closing connection" + ConsoleColors.RESET, e);
        }
    }    

    @Override
    public String getNodeName() {
        return this.name;
    }

    @Override
    public String getNodeHost() {
        return this.host;
    }

    @Override
    public int getNodePort() {
        return this.port;
    }

    public Socket getServerSocket() {
        return this.serverSocket;
    }

    public BigInteger getNodeIdentifier() {
        return this.identifier;
    }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {
            this.hashStartRange.toString(),
            this.hashEndRange.toString()
        };
    }

    public BigInteger[] getNodeHashRangeBigInt() {
        return new BigInteger[] {
            this.hashStartRange,
            this.hashEndRange
        };
    }

    public void setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setNodeHashRange(String[] hashRange){
        this.hashStartRange = new BigInteger(hashRange[0]);
        this.hashEndRange = new BigInteger(hashRange[1]);
    }

    public void setNodeHashRange(BigInteger start, BigInteger end) {
        this.hashStartRange = start;
        this.hashEndRange = end;
    }

    public void setNodeHashRange(String start, String end) {
        this.hashStartRange = new BigInteger(start, 16);
        this.hashEndRange = new BigInteger(end, 16);
    }

    public BigInteger getNodeHashStartRange() {
        return this.hashStartRange;
    }

    public BigInteger getNodeHashEndRange() {
        return this.hashEndRange;
    }

    public void setNodeHashStartRange(BigInteger start) {
        this.hashStartRange = start;
    }

    public void setNodeHashEndRange(BigInteger end) {
        this.hashEndRange = end;
    }

    public void setNodeName(String name) {
        this.name = name;
    }

    public void setNodeHost(String host) {
        this.host = host;
    }

    public void setNodePort(Integer port) {
        this.port = port;
    }

    public void setNodeIdentifier(BigInteger identifier) {
        this.identifier = identifier;
    }

    public ObjectOutputStream getObjectOutputStream(){
        return this.ecsOutStream;
    }


    public static boolean isKeyInRange(BigInteger keyHash, BigInteger start, BigInteger end) {
        // if start > end, then the range wraps around the hashring
        if (start.compareTo(end) >= 0) {
            // if the key is in the range of RING_START to end or start to RING_END
            if ((keyHash.compareTo(RING_START) >= 0) && (keyHash.compareTo(end) <= 0)
                    || (keyHash.compareTo(start) >= 0) && (keyHash.compareTo(RING_END) <= 0)) {
                return true;
            } else {
                return false;
            }
        } else {
            return keyHash.compareTo(start) >= 0 && keyHash.compareTo(end) <= 0;
        }
    }

    public static boolean isKeyInRange(String key, BigInteger start, BigInteger end) {
        return isKeyInRange(MD5.getHash(key), start, end);
    }

    public boolean isKeyInRange(BigInteger keyHash) {
        return isKeyInRange(keyHash, this.hashStartRange, this.hashEndRange);
    }

    public boolean isKeyInRange(String key) {
        return isKeyInRange(MD5.getHash(key), this.hashStartRange, this.hashEndRange);
    }

    public String toString() {
        return this.hashStartRange.toString(16) + "," + this.hashEndRange.toString(16) + "," + this.host + ":" + this.port;
    }

    public String keyrangeRead(ECSNode firstReplica, ECSNode secondReplica) {
        BigInteger hashStart = this.hashEndRange;
        if (firstReplica != null) {
            System.out.println("Updating end range to (first)" + firstReplica.getNodeHashEndRange().toString(16) + ",");
            hashStart = firstReplica.getNodeHashStartRange();
        }
        if (secondReplica != null) {
            System.out.println("Updating end range to (second)" + secondReplica.getNodeHashEndRange().toString(16) + ",");
            hashStart = secondReplica.getNodeHashStartRange();
        }
        
        return hashStart.toString(16) + "," + this.hashEndRange.toString(16) + "," + this.host + ":" + this.port;
    }
}
