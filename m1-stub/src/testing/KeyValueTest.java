package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;


public class KeyValueTest extends TestCase {

	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPutEmptyKey() {
		String key = "";
		String value = "some_val";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.INVALID_KEY);
	}
	
	@Test
	public void testPutEmptyValue() {
		String key = "some_key";
		String value = "";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}
	
	@Test
	public void testPutEmptyKeyAndValue() {
		String key = "";
		String value = "";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.INVALID_KEY);
	}
	
	@Test
	public void testPutKeyWithSpaces() {
		String key = "key with spaces";
		String value = "some_val";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testGetKeyWithSpaces() {
		String key = "key with spaces";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
	}
	
	@Test
	public void testPutKeyWithNewlines() {
		String key = "key\nwith\n";
		String value = "some_val";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	

	@Test
	public void testPutValueWithNewlines() {
		String key = "key";
		String value = "some\nval\nwith newline";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutKeyAndValueWithNewlines() {
		String key = "key\nwith\nnewlines";
		String value = "value\nwith\nnewlines";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE);
	}
	
	@Test
	public void testPutKeyMaxLen() {
		String key = "a".repeat(20);
		String value = "some_val";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutKeyTooLong() {
		String key = "a".repeat(21);
		String value = "some_val";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(KVMessage.StatusType.INVALID_KEY, response.getStatus());
	}
	
	@Test
	public void testGetKeyMaxLen() {
		String key = "a".repeat(20);
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}
		
		assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS);
	}
	
	@Test
	public void testGetKeyTooLong() {
		String key = "a".repeat(21);
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(KVMessage.StatusType.INVALID_KEY, response.getStatus());
	}
}
