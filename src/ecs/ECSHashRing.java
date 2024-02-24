package ecs;

import java.math.BigInteger;
import java.util.TreeMap;

import shared.MD5;

public class ECSHashRing {
    private TreeMap<BigInteger, ECSNode> hashring;

    public ECSHashRing() {
        this.hashring = new TreeMap<BigInteger, ECSNode>();
    }

    //TODO transferring kv pairs to new node
    public void addNode(ECSNode node) {
        this.hashring.put(node.getNodeIdentifier(), node);

        if (this.hashring.size() == 1) {
            node.setNodeHashRange(node.getNodeIdentifier(), node.getNodeIdentifier());
            return;
        }

        ECSNode next = this.hashring.higherEntry(node.getNodeIdentifier()).getValue();
        // if the new node is the last node in the hashring, then its next node is
        // the first node in the hashring
        if (next == null) {
            next = this.hashring.firstEntry().getValue();
        }
        // set the start of the next node's range to the new node's hash
        next.setNodeHashStartRange(node.getNodeIdentifier());

        ECSNode prev = this.hashring.lowerEntry(node.getNodeIdentifier()).getValue();

        node.setNodeHashStartRange(prev.getNodeIdentifier());
        node.setNodeHashEndRange(node.getNodeIdentifier());
    }

    //TODO transferring kv pairs to preexisting node
    public void removeNode(ECSNode node) {
        if (this.hashring.size() == 1) {
            this.hashring.remove(node.getNodeIdentifier());
            return;
        }

        ECSNode next = this.hashring.higherEntry(node.getNodeIdentifier()).getValue();
        // if the removed node is the last node in the hashring, then its next node is
        // the first node in the hashring
        if (next == null) {
            next = this.hashring.firstEntry().getValue();
        }
        // set the start of the next node's range to the removed node's range start
        next.setNodeHashStartRange(node.getNodeHashStartRange());

        this.hashring.remove(node.getNodeIdentifier());
    }

    public ECSNode getNodeForKey(String key) {
        BigInteger keyHash = MD5.getHash(key);
        ECSNode node = this.hashring.ceilingEntry(keyHash).getValue();
        // if key is greater than the largest node hash in the hashring, then the
        // first node in the hashring is returned
        if (node == null) {
            node = this.hashring.firstEntry().getValue();
        }
        return node;
    }

    public ECSNode getNodeByHash(BigInteger hash) {
        return this.hashring.ceilingEntry(hash).getValue();
    }

    public ECSNode getFirstNode() {
        return this.hashring.firstEntry().getValue();
    }

    public ECSNode getLastNode() {
        return this.hashring.lastEntry().getValue();
    }

    public TreeMap<BigInteger, ECSNode> getHashring() {
        return this.hashring;
    }

    public void setHashring(TreeMap<BigInteger, ECSNode> hashring) {
        this.hashring = hashring;
    }

    public void clear() {
        this.hashring.clear();
    }

    public boolean isEmpty() {
        return this.hashring.isEmpty();
    }

    public int size() {
        return this.hashring.size();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ECSNode node : this.hashring.values()) {
            sb.append(node.toString() + " ");
        }
        return sb.toString();
    }
}
