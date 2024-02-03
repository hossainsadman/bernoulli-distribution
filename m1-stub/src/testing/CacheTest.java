package testing;

import java.net.UnknownHostException;

import app_kvServer.KVServer;

import junit.framework.TestCase;


public class CacheTest extends TestCase {

	
	public void testLRUCache() {
		Exception ex = null;
		
        KVServer LRUServer = new KVServer(20010, 3, "LRU");
		try {
	        LRUServer.clearStorage();
	        LRUServer.printStorageAndCache();
	        LRUServer.putKV("1", "1");
	        LRUServer.putKV("2", "2");
	        LRUServer.putKV("3", "3");
	        System.out.println(LRUServer.getKV("1")); 
	        LRUServer.putKV("4", "4"); // [1, 3, 4]
	        LRUServer.printStorageAndCache();
	        LRUServer.clearCache();
	        LRUServer.printStorageAndCache(); // should be empty
	        LRUServer.putKV("1", "1");
	        LRUServer.printStorageAndCache(); // should be empty			
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	public void testLFUCache() {
		Exception ex = null;
		
        KVServer LFUServer = new KVServer(20010, 3, "LFU");
        try {
            LFUServer.clearStorage();
            LFUServer.printStorageAndCache();
            LFUServer.putKV("1", "1");
            LFUServer.putKV("2", "2");
            LFUServer.putKV("3", "3");
            System.out.println(LFUServer.getKV("1")); // freq of 1 = 2
            System.out.println(LFUServer.getKV("3")); // freq of 3 = 2
            System.out.println(LFUServer.getKV("3")); // freq of 3 = 3
            LFUServer.putKV("4", "4");
            LFUServer.printStorageAndCache(); // should be 1, 3, 4
            LFUServer.clearCache();
            LFUServer.printStorageAndCache(); // should be 1, 3, 4
        } catch (Exception e) {
			ex = e;
        }
		
		assertNull(ex);
	}
	
	public void testFIFOCache() {
		Exception ex = null;
		
        KVServer FIFOServer = new KVServer(20010, 3, "FIFO");
        try {
            FIFOServer.clearStorage();
            FIFOServer.printStorageAndCache();
            FIFOServer.putKV("1", "1");
            FIFOServer.putKV("2", "2");
            FIFOServer.putKV("3", "3");
            System.out.println(FIFOServer.getKV("1")); 
            System.out.println(FIFOServer.getKV("3")); 
            System.out.println(FIFOServer.getKV("3")); // SHOULD NOT CHANGE ANYTHING
            FIFOServer.putKV("4", "4");
            FIFOServer.printStorageAndCache(); // should be 2, 3, 4
            FIFOServer.clearCache();
            FIFOServer.printStorageAndCache(); // should be empty
        } catch (Exception e) {
			ex = e;
        }

		assertNull(ex);
	}
}
