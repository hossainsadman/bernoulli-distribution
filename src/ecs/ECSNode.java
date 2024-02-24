package ecs;

import java.math.BigInteger;

import shared.MD5;

public class ECSNode implements IECSNode {
    private String name;
    private String host;
    private Integer port;
    private BigInteger identifier;
    private BigInteger hashStartRange;
    private BigInteger hashEndRange;

    public static final BigInteger RING_START = BigInteger.ZERO;
    public static final BigInteger RING_END = new BigInteger(String.valueOf('F').repeat(32), 16);

    public ECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.identifier = MD5.getHash(host + ":" + port);
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
        return this.hashStartRange.toString() + "," + this.hashEndRange.toString() + ";" + this.host + ":" + this.port;
    }
}
