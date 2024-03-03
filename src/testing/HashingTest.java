package testing;

import java.net.UnknownHostException;
import java.math.BigInteger;

import ecs.ECSNode;

import junit.framework.TestCase;


public class HashingTest extends TestCase {

	
	public void testInHashRangeEnds() {
		Exception ex = null;
		
        BigInteger mid = new BigInteger(String.valueOf('A').repeat(32), 16);
		try {
	        assertTrue(ECSNode.isKeyInRange(mid, ECSNode.RING_START, ECSNode.RING_END));
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	public void testInHashRangeMidSimple() {
		Exception ex = null;
		
        BigInteger mid = new BigInteger(String.valueOf('B').repeat(32), 16);
        
        BigInteger low = new BigInteger(String.valueOf('A').repeat(32), 16);
        BigInteger hi  = new BigInteger(String.valueOf('C').repeat(32), 16);
		try {
	        assertTrue(ECSNode.isKeyInRange(mid, low, hi));
		} catch (Exception e) {
			ex = e;
		}	

        mid = new BigInteger(String.valueOf('2').repeat(32), 16);

        low = new BigInteger(String.valueOf('1').repeat(32), 16);
        hi  = new BigInteger(String.valueOf('3').repeat(32), 16);
		try {
	        assertTrue(ECSNode.isKeyInRange(mid, low, hi));
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	public void testNotInHashRangeInverted() {
		Exception ex = null;
		
        BigInteger hash  = new BigInteger(String.valueOf('1').repeat(32), 16);

        BigInteger start = new BigInteger(String.valueOf('3').repeat(32), 16);
        BigInteger end   = new BigInteger(String.valueOf('2').repeat(32), 16);
		try {
	        assertTrue(ECSNode.isKeyInRange(hash, start, end));
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	// public void testLRUCache() {
	// 	Exception ex = null;
		
    //     KVServer LRUServer = new KVServer(20010, 3, "LRU");
	// 	try {
	//         LRUServer.clearStorage();
	//         LRUServer.putKV("1", "1");
	//         LRUServer.putKV("2", "2");
	//         LRUServer.putKV("3", "3");
	//         assertTrue(LRUServer.getKV("1").equals("1")); 
	//         assertTrue(LRUServer.getKV("2").equals("2")); 
	//         LRUServer.putKV("4", "4"); // [1, 2, 4]
    //         assertTrue(LRUServer.getKV("4").equals("4"));

    //         LRUServer.putKV("3", "null"); // Removing key "3" from storage
        
    //         boolean exceptionOccurs = false;
    //         try {
    //             LRUServer.getKV("3");
    //         } catch (Exception e2) {
    //             exceptionOccurs = true;
    //         }
    //         assertTrue(exceptionOccurs);
            
	//         assertTrue(LRUServer.getKV("1").equals("1")); 
	//         assertTrue(LRUServer.getKV("2").equals("2")); 
    //         LRUServer.clearCache();
    //         assertTrue(LRUServer.getKV("1").equals("1"));
    //         LRUServer.clearStorage();
    //         exceptionOccurs = false;
    //         try {
    //             LRUServer.getKV("1");
    //         } catch (Exception e2) {
    //             exceptionOccurs = true;
    //         }
    //         assertTrue(exceptionOccurs);

	//         LRUServer.putKV("1", "1");
	//         assertTrue(LRUServer.getKV("1").equals("1"));			
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}	
		
	// 	assertNull(ex);
	// }
}
