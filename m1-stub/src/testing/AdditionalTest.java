package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
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
    public void testGetKeyWithNewlines() {
        String key = "key\nwith\nnewlines";
        String value = "val";
        KVMessage resPut = null;
        KVMessage resGet = null;
        Exception ex = null;

        try {
            resPut = kvClient.put(key, value);
            resGet = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && resPut.getStatus() == StatusType.PUT_SUCCESS 
            && resGet.getStatus() == StatusType.GET_SUCCESS
            && resGet.getKey().equals(key)
            && resGet.getValue().equals(value));
    }
 
    @Test
    public void testGetKeyAndValueWithNewlines() {
        String key = "key\nwith\nnewlines2";
        String value = "value\nwith\nnewlines";
        KVMessage resPut = null;
        KVMessage resGet = null;
        Exception ex = null;

        try {
            resPut = kvClient.put(key, value);
            resGet = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && resPut.getStatus() == StatusType.PUT_SUCCESS
                && resGet.getStatus() == StatusType.GET_SUCCESS
                && resGet.getKey().equals(key)
                && resGet.getValue().equals(value));
    }
    
    public void testUpdateAndDelete() {
        String key = "updateAndDelVal";
        String initialValue = "this is first";
        String updatedValue = "this is second";

        KVMessage resUpdate = null;
        KVMessage resDelete = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            resUpdate = kvClient.put(key, updatedValue);
            resDelete = kvClient.put(key, "null");
        } catch (Exception e) {
            ex = e;
        }
        System.out.println("123");
        System.out.println(resUpdate.getKey() + ", " + resUpdate.getValue() + ", "  + resUpdate.getStatus());
        System.out.println(resDelete.getKey() + ", " + resDelete.getValue() + ", "  + resDelete.getStatus());
        assertTrue(ex == null && resUpdate.getStatus() == StatusType.PUT_UPDATE 
                && resDelete.getStatus() == StatusType.DELETE_SUCCESS
                && resUpdate.getKey().equals(key)
                && resUpdate.getValue().equals(updatedValue)
                && resDelete.getKey().equals(key));
    }
}
