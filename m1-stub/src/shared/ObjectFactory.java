package shared;

import app_kvClient.IKVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;

public final class ObjectFactory {
	/*
	 * Creates a KVClient object for auto-testing purposes
	 */
    public static IKVClient createKVClientObject() {
        // TODO Auto-generated method stub
    	return null;
    }
    
    /*
     * Creates a KVServer object for auto-testing purposes
     */
	public static IKVServer createKVServerObject(int port, int cacheSize, String strategy) {
        // TODO Auto-generated method stub
        KVServer server = new KVServer(20010, 3, "LRU");
		return null;
	}
}