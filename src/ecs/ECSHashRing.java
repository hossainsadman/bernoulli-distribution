package ecs;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.TreeMap;
import java.util.Map;

import shared.MD5;

public class ECSHashRing implements Serializable {
    private TreeMap<BigInteger, ECSNode> hashring;

    public ECSHashRing() {
        this.hashring = new TreeMap<BigInteger, ECSNode>();
    }

    //TODO transferring kv pairs to new node
    public ECSNode addNode(ECSNode node) {
        this.hashring.put(node.getNodeIdentifier(), node);
        System.out.println(this.hashring.size() + " nodes in hashring");

        if (this.hashring.size() == 1) {
            node.setNodeHashRange(node.getNodeIdentifier(), node.getNodeIdentifier());
            return null; // no next node (to get kv pairs from)
        }

        Map.Entry<BigInteger, ECSNode> nextEntry = this.hashring.higherEntry(node.getNodeIdentifier());
        // if the new node is the last node in the hashring, then its next node is
        // the first node in the hashring
        ECSNode next = (nextEntry != null) ? nextEntry.getValue() : this.hashring.firstEntry().getValue();

        // set the start of the next node's range to the new node's hash
        next.setNodeHashStartRange(node.getNodeIdentifier());

        Map.Entry<BigInteger, ECSNode> prevEntry = this.hashring.lowerEntry(node.getNodeIdentifier());
        // if the new node is the first node in the hashring, then its previous node is
        // the last node in the hashring
        ECSNode prev = (prevEntry != null) ? prevEntry.getValue() : this.hashring.lastEntry().getValue();

        // set the start of the new node's range to the previous node's hash and the end to the new node's hash
        node.setNodeHashStartRange(prev.getNodeIdentifier());
        node.setNodeHashEndRange(node.getNodeIdentifier());

        return next;
    } 

    //TODO transferring kv pairs to preexisting node
    public ECSNode removeNode(ECSNode node) {
        if (this.hashring.size() == 1) {
            this.hashring.remove(node.getNodeIdentifier());
            return null; // no next node (to transfer kv pairs to)
        }

        Map.Entry<BigInteger, ECSNode> nextEntry = this.hashring.higherEntry(node.getNodeIdentifier());
        // if the removed node is the last node in the hashring, then its next node is
        // the first node in the hashring
        ECSNode next = (nextEntry != null) ? nextEntry.getValue() : this.hashring.firstEntry().getValue();

        assert(next != null);
        assert(next != node);

        // set the start of the next node's range to the removed node's range start
        next.setNodeHashStartRange(node.getNodeHashStartRange());

        this.hashring.remove(node.getNodeIdentifier());

        return next;
    }

    public ECSNode getNodeForKey(String key) {
        BigInteger keyHash = MD5.getHash(key);
        Map.Entry<BigInteger, ECSNode> foundEntry = this.hashring.ceilingEntry(keyHash);
        // if key is greater than the largest node hash in the hashring, then the
        // first node in the hashring is returned
        ECSNode node = (foundEntry != null) ? foundEntry.getValue() : this.hashring.firstEntry().getValue();
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
