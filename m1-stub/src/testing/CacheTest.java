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
	        LRUServer.putKV("1", "1");
	        LRUServer.putKV("2", "2");
	        LRUServer.putKV("3", "3");
	        assertTrue(LRUServer.getKV("1").equals("1")); 
	        assertTrue(LRUServer.getKV("2").equals("2")); 
	        LRUServer.putKV("4", "4"); // [1, 2, 4]
            assertTrue(LRUServer.getKV("4").equals("4"));

            LRUServer.putKV("3", "null"); // Removing key "3" from storage
            LRUServer.printStorageAndCache();
        
            boolean exceptionOccurs = false;
            try {
                LRUServer.getKV("3");
            } catch (Exception e2) {
                exceptionOccurs = true;
            }
            assertTrue(exceptionOccurs);
            
	        assertTrue(LRUServer.getKV("1").equals("1")); 
	        assertTrue(LRUServer.getKV("2").equals("2")); 
            LRUServer.clearCache();
            assertTrue(LRUServer.getKV("1").equals("1"));
            LRUServer.clearStorage();
            exceptionOccurs = false;
            try {
                LRUServer.getKV("1");
            } catch (Exception e2) {
                exceptionOccurs = true;
            }
            assertTrue(exceptionOccurs);

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
            LFUServer.putKV("1", "1");
            LFUServer.putKV("2", "2");
            LFUServer.putKV("3", "3");
            assertTrue(LFUServer.getKV("1").equals("1")); // freq of 1 = 2
            assertTrue(LFUServer.getKV("3").equals("3")); // freq of 3 = 2
            assertTrue(LFUServer.getKV("3").equals("3")); // freq of 3 = 3
            LFUServer.putKV("4", "4"); // should be 1, 3, 4
            assertTrue(LFUServer.getKV("2").equals("2"));

            LFUServer.clearStorage();
            boolean exceptionOccurs = false;
            try {
                LFUServer.getKV("2");
            } catch (Exception e2) {
                exceptionOccurs = true;
            }
            assertTrue(exceptionOccurs);
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
            FIFOServer.putKV("1", "1");
            FIFOServer.putKV("2", "2");
            FIFOServer.putKV("3", "3");
            assertTrue(FIFOServer.getKV("1").equals("1")); 
            assertTrue(FIFOServer.getKV("2").equals("2")); 
            assertTrue(FIFOServer.getKV("3").equals("3"));
            
            boolean exceptionOccurs = false;
            try {
                FIFOServer.getKV("4");
            } catch (Exception e2) {
                exceptionOccurs = true;
            }
            assertTrue(exceptionOccurs);
            
            FIFOServer.putKV("4", "4");
            assertTrue(FIFOServer.getKV("4").equals("4")); 
            assertTrue(FIFOServer.getKV("2").equals("2")); 
            FIFOServer.clearCache();
            assertTrue(FIFOServer.getKV("2").equals("2"));

            FIFOServer.clearStorage();
            exceptionOccurs = false;
            try {
                FIFOServer.getKV("1");
            } catch (Exception e2) {
                exceptionOccurs = true;
            }
            assertTrue(exceptionOccurs);

        } catch (Exception e) {
			ex = e;
        }

		assertNull(ex);
	}
}
