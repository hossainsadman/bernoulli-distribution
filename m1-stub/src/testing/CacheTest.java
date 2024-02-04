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
	        assertTrue(LRUServer.getKV("1").equals("1")); 
	        assertTrue(LRUServer.getKV("2").equals("2")); 
	        assertTrue(LRUServer.getKV("3").equals("3")); 
	        LRUServer.putKV("4", "4"); // [1, 3, 4]
	        assertTrue(LRUServer.getKV("4").equals("4")); 
	        assertTrue(LRUServer.getKV("2").equals(null)); 
	        assertTrue(LRUServer.getKV("1").equals("1")); 
	        assertTrue(LRUServer.getKV("3").equals("3")); 
	        LRUServer.clearCache();
            assertTrue(LRUServer.getKV("1").equals(null)); 
	        LRUServer.putKV("1", "1");
	        assertTrue(LRUServer.getKV("1").equals("1"));			
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
            assertTrue(LFUServer.getKV("1").equals("1")); // freq of 1 = 2
            assertTrue(LFUServer.getKV("3").equals("3")); // freq of 3 = 2
            assertTrue(LFUServer.getKV("3").equals("3")); // freq of 3 = 3
            LFUServer.putKV("4", "4");
            LFUServer.printStorageAndCache(); // should be 1, 3, 4
            assertTrue(LFUServer.getKV("2").equals(null));
            assertTrue(LFUServer.getKV("4").equals("4")); // freq of 4 = 2
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
            assertTrue(FIFOServer.getKV("1").equals("1")); 
            assertTrue(FIFOServer.getKV("2").equals("2")); 
            assertTrue(FIFOServer.getKV("3").equals("3")); 
            assertTrue(FIFOServer.getKV("4").equals(null)); 
            FIFOServer.putKV("4", "4");
            assertTrue(FIFOServer.getKV("4").equals("4")); 
            assertTrue(FIFOServer.getKV("2").equals("2")); 
            FIFOServer.clearCache();
            assertTrue(FIFOServer.getKV("1").equals("1")); 
        } catch (Exception e) {
			ex = e;
        }

		assertNull(ex);
	}
}
