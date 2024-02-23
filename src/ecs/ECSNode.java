package ecs;

import java.math.BigInteger;

import shared.MD5;

public class ECSNode implements IECSNode {
    private String name;
    private String host;
    private Integer port;
    private BigInteger hash;

    public ECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.hash = MD5.getHash(host + ":" + port);
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

    public BigInteger getNodeHash() {
        if (this.hash == null) {
            this.hash = MD5.getHash(this.host + ":" + this.port);
        }
        return this.hash;
    }
    
    @Override
    public String[] getNodeHashRange() {
        return new String[] {""};
    }
}
