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

    public ECSNode addNode(ECSNode node) {
        System.out.println("Adding node ; " + node.getNodeIdentifier());
        this.hashring.put(node.getNodeIdentifier(), node);
        System.out.println(this.hashring.size() + " nodes in hashring");

        if (this.hashring.size() == 1) {
            node.setNodeHashStartRange(node.getNodeIdentifier());
            node.setNodeHashEndRange(node.getNodeIdentifier());
            return null; // no next node (to get kv pairs from)
        }

        ECSNode successor = getNodeSuccessor(node.getNodeIdentifier());
        ECSNode predecessor = getNodePredecessor(node.getNodeIdentifier());

        node.setNodeHashRange(predecessor.getNodeHashEndRange(), node.getNodeIdentifier());
        successor.setNodeHashStartRange(node.getNodeIdentifier());

        // Transfer KV pairs from predecessor to new node
        return successor;
    } 

    public ECSNode removeNode(ECSNode node) {
        if (this.hashring.size() == 1) {
            this.hashring.remove(node.getNodeIdentifier());
            return null; // no next node (to transfer kv pairs to)
        }

        ECSNode successor = getNodeSuccessor(node.getNodeIdentifier());
        // set the start of the next node's range to the removed node's range start
        successor.setNodeHashStartRange(node.getNodeHashStartRange());
        this.hashring.remove(node.getNodeIdentifier());

        return successor;
    }

    public ECSNode getNodePredecessor(BigInteger nodeIdentifier){
        Map.Entry<BigInteger, ECSNode> prevEntry = this.hashring.lowerEntry(nodeIdentifier);
        // if the new node is the first node in the hashring, then its previous node is
        // the last node in the hashring
        return (prevEntry != null) ? prevEntry.getValue() : this.hashring.lastEntry().getValue();
    }

    public ECSNode getNodeSuccessor(BigInteger nodeIdentifier){
        Map.Entry<BigInteger, ECSNode> nextEntry = this.hashring.higherEntry(nodeIdentifier);
        // if the new node is the last node in the hashring, then its next node is
        // the first node in the hashring
        return (nextEntry != null) ? nextEntry.getValue() : this.hashring.firstEntry().getValue();
    }

    public ECSNode getNodeForKey(String key) {
        BigInteger keyHash = MD5.getHash(key);
        Map.Entry<BigInteger, ECSNode> foundEntry = this.hashring.ceilingEntry(keyHash);
        // if key is greater than the largest node hash in the hashring, then the
        // first node in the hashring is returned
        ECSNode node = (foundEntry != null) ? foundEntry.getValue() : this.hashring.firstEntry().getValue();
        return node;
    }

    public ECSNode getNodeForIdentifier(BigInteger identifierHash){
        Map.Entry<BigInteger, ECSNode> foundEntry = null;
        for (Map.Entry<BigInteger, ECSNode> entry : hashring.entrySet()) {
            ECSNode node = entry.getValue();
            if (node.getNodeIdentifier().equals(identifierHash)) {
                foundEntry = entry;
                break; // Found the node, exit the loop
            }
        }

        ECSNode node = (foundEntry != null) ? foundEntry.getValue() : this.hashring.firstEntry().getValue();
        return node;
    }

    public ECSNode getNodeForIdentifier(String identifier){
        BigInteger identifierHash = MD5.getHash(identifier);
        Map.Entry<BigInteger, ECSNode> foundEntry = null;
        for (Map.Entry<BigInteger, ECSNode> entry : hashring.entrySet()) {
            ECSNode node = entry.getValue();
            if (node.getNodeIdentifier().equals(identifierHash)) {
                foundEntry = entry;
                break; // Found the node, exit the loop
            }
        }

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

    public ECSNode[] getNextTwoNodeSuccessors(ECSNode node){
        ECSNode[] result = new ECSNode[]{null, null};
        BigInteger identifier = node.getNodeIdentifier();
        
        ECSNode firstSuccessor = this.getNodeSuccessor(identifier);
        if (firstSuccessor != null && !firstSuccessor.getNodeIdentifier().equals(identifier)){
            result[0] = firstSuccessor;

            ECSNode secondSuccessor = this.getNodeSuccessor(firstSuccessor.getNodeIdentifier());

            if (secondSuccessor != null && !secondSuccessor.getNodeIdentifier().equals(identifier)){
                result[1] = secondSuccessor;
            }
        }

        return result;
    }

    public String keyrangeRead(){
        StringBuilder sb = new StringBuilder();
        for (ECSNode node: this.hashring.values()){
            ECSNode[] successors = this.getNextTwoNodeSuccessors(node);
            sb.append(node.keyrangeRead(successors[0], successors[1]) + ";");
        }

        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (ECSNode node : this.hashring.values()) {
            sb.append(node.toString() + ";");
        }
        return sb.toString();
    }
}
