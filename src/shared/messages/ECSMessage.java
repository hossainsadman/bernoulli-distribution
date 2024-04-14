package shared.messages;

import java.io.Serializable;
import java.util.HashMap;


import java.util.List;
/**
 * KVServer <-> ECS message Protocol
 * 
 * INIT <server-name:String> 
 * - Called when KVServer connects to ECS
 * 
 * HASHRING <hashring:ECSHashRing>
 * - Update hashring on KVServer
 * 
 * TRANSFER_FROM <toNode:ECSNode>
 * - Sent from ECS -> KVServer to allow KVServer to tranfer nodes to specified toNode
 * 
 * TRANSFER_TO <toNode:ECSNode> <kvPairs:HashMap<String, String>>
 * - Sent from KVServer -> ECS to trigger transfer of kvPairs to toNode
 * - Sent after TRANSFER_FROM from the node tranferring kvPairs to the node receiving them
 * - TRANSFER_TO.toNode == TRANSFER_FROM.toNode
 * 
 * RECEIVE <fromNode:ECSNode> <kvPairs:HashMap<String, String>>
 * - Sent from ECS -> KVServer that is getting kvPairs tranfered to
 * - Sent after TRANSFER_TO command received from KVServer
 * - Sent is the fromNode that is transferring the kvPairs
 * 
 * TRANSFER_COMPLETE <pingNode:ECSNode>
 * - Sent after the keys have been transfered to a specific KVServer from pingServer, and pingServer is notified
 * - Sent after the KVServer processes the RECEIVE command
 * - TRANSFER_COMPLETE.pingNode == RECEIVE.fromNode
 */
public class ECSMessage implements Serializable {

    public enum ECSMessageType {
        INIT(List.of("SERVER_NAME")),
        HASHRING(List.of("HASHRING")),
        TRANSFER_FROM(List.of("TO_NODE")),
        TRANSFER_TO(List.of("TO_NODE", "KV_PAIRS", "SQL_TABLES")),
        RECEIVE(List.of("FROM_NODE", "KV_PAIRS", "SQL_TABLES")),
        TRANSFER_COMPLETE(List.of("PING_NODE")),
        SHUTDOWN(List.of("KV_PAIRS", "SQL_TABLES")),
        SHUTDOWN_SERVER(List.of());
        
        private final List<String> requiredParameters;

        ECSMessageType(List<String> requiredParameters) {
            this.requiredParameters = requiredParameters;
        }

        public List<String> getRequiredParameters() {
            return requiredParameters;
        }
    }

    private ECSMessageType type;
    private HashMap<String, Serializable> parameters;

    public ECSMessage(ECSMessageType type, HashMap<String, Serializable> parameters) {
        this.type = type;
        this.parameters = parameters;
    }

    public ECSMessage(ECSMessageType type) {
        this.type = type;
        this.parameters = new HashMap<>();
    }

    public void addParameter(String key, Serializable value) {
        parameters.put(key, value);
    }

    public Serializable getParameter(String key) {
        return parameters.get(key);
    }

    public ECSMessageType getType() {
        return type;
    }
    
    // Method to check if all required parameters are set
    public boolean areAllRequiredParametersSet() {
        return type.getRequiredParameters().stream().allMatch(parameters::containsKey);
    }
}
