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

    // public BigInteger getNodeHash() {
    // if (this.hashStartRange == null) {
    // this.hashStartRange = MD5.getHash(this.host + ":" + this.hashStartRange);
    // }
    // return this.hashStartRange;
    // }

    @Override
    public String[] getNodeHashRange() {
        return new String[] {
            hashStartRange.toString(),
            hashEndRange.toString()
        };
    }
}
