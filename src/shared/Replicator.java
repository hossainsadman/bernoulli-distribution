package shared;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;

import app_kvServer.KVServer;
import ecs.ECSHashRing;
import ecs.ECSNode;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage.StatusType;

public class Replicator {
    
    public KVServer server;

    private CommunicationService firstReplicaConn;
    private String firstReplicaName;
    private BigInteger firstReplicaHash;
    private BigInteger[] firstReplicaHashRange;
    private ECSNode firstReplicaEcsNode;

    private CommunicationService secondReplicaConn;
    private String secondReplicaName;
    private BigInteger secondReplicaHash;
    private BigInteger[] secondReplicaHashRange;
    private ECSNode secondReplicaEcsNode;

    public Replicator(KVServer server){
        this.server = server;
    }

    public boolean replicate(String key, String value) throws Exception{
        BasicKVMessage replicateMessage = new BasicKVMessage(StatusType.REPLICATE, key, value);

        if (this.firstReplicaConn != null){
            this.firstReplicaConn.sendMessage(replicateMessage);

            BasicKVMessage response = this.firstReplicaConn.receiveMessage();
            if (response.getStatus() != StatusType.REPLICATE_SUCCESS){
                System.out.println("Received " + response.getStatus() + " instead of REPLICATE_SUCCESS from first replica");
                return false;
            }
        }

        if (this.secondReplicaConn != null){
            this.secondReplicaConn.sendMessage(replicateMessage);

            BasicKVMessage response = this.secondReplicaConn.receiveMessage();
            if (response.getStatus() != StatusType.REPLICATE_SUCCESS){
                System.out.println("Received " + response.getStatus() + " instead of REPLICATE_SUCCESS from second replica");
                return false;
            }
        }

        return true;
    }

    public void connect(ECSHashRing hashRing) {
        if (hashRing == null) return;

        this.disconnect(); // disconnect if there are existing connections
        ECSNode[] successors = hashRing.getNextTwoNodeSuccessors(hashRing.getNodeForIdentifier(this.server.getStringIdentifier()));

        if (successors[0] != null){
            System.out.println("First replica: " + successors[0].getNodeName());
            ECSNode firstSuccessor = successors[0];
            this.firstReplicaHash = firstSuccessor.getNodeIdentifier();
            this.firstReplicaHashRange = firstSuccessor.getNodeHashRangeBigInt();
            this.firstReplicaConn = this.connectToServer(firstSuccessor);
            this.firstReplicaEcsNode = hashRing.getNodeForIdentifier(this.firstReplicaHash);
        }

        if (successors[1] != null){
            System.out.println("Second replica: " + successors[1].getNodeName());
            ECSNode secondSuccessor = successors[1];
            this.secondReplicaHash = secondSuccessor.getNodeIdentifier();
            this.secondReplicaHashRange = secondSuccessor.getNodeHashRangeBigInt();
            this.secondReplicaConn = this.connectToServer(secondSuccessor);
            this.secondReplicaEcsNode = hashRing.getNodeForIdentifier(this.secondReplicaHash);
        }
    }

    public void connect(){
        ECSHashRing hashRing = this.server.getHashRing();
        if (hashRing == null) return;

        this.disconnect(); // disconnect if there are existing connections
        ECSNode[] successors = hashRing.getNextTwoNodeSuccessors(hashRing.getNodeForIdentifier(this.server.getStringIdentifier()));

        if (successors[0] != null){
            System.out.println("First replica: " + successors[0].getNodeName());
            ECSNode firstSuccessor = successors[0];
            this.firstReplicaHash = firstSuccessor.getNodeIdentifier();
            this.firstReplicaHashRange = firstSuccessor.getNodeHashRangeBigInt();
            this.firstReplicaConn = this.connectToServer(firstSuccessor);
            this.firstReplicaEcsNode = hashRing.getNodeForIdentifier(this.firstReplicaHash);
        }

        if (successors[1] != null){
            System.out.println("Second replica: " + successors[1].getNodeName());
            ECSNode secondSuccessor = successors[1];
            this.secondReplicaHash = secondSuccessor.getNodeIdentifier();
            this.secondReplicaHashRange = secondSuccessor.getNodeHashRangeBigInt();
            this.secondReplicaConn = this.connectToServer(secondSuccessor);
            this.secondReplicaEcsNode = hashRing.getNodeForIdentifier(this.secondReplicaHash);
        }
    }

    public CommunicationService getFirstReplicaConn(){
        return this.firstReplicaConn;
    }

    public CommunicationService getSecondReplicaConn(){
        return this.secondReplicaConn;
    }   

    private CommunicationService connectToServer(ECSNode serverNode){
        System.out.println("Connecting to " + serverNode.getNodeHost() + ":" + serverNode.getNodePort());

        Socket serverSocket = null;
        try {
            serverSocket = new Socket(serverNode.getNodeHost(), serverNode.getNodePort());
        } catch (Exception e) {
            System.out.println("[Replicator] Error creating socket to ecsnode");
            return null;
        }   

        return new CommunicationService(serverSocket);
    }

    public void disconnect(){
        if (firstReplicaConn != null) firstReplicaConn.disconnect();
        if (secondReplicaConn != null) secondReplicaConn.disconnect();
    }

}
