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
        String key = "updateAndDeleteValue";
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

        System.out.println(resUpdate.getKey() + ", " + resUpdate.getValue() + ", " + resUpdate.getStatus());
        System.out.println(resDelete.getKey() + ", " + resDelete.getValue() + ", " + resDelete.getStatus());
        assertTrue(ex == null && resUpdate.getStatus() == StatusType.PUT_SUCCESS
                && resDelete.getStatus() == StatusType.DELETE_SUCCESS
                && resUpdate.getKey().equals(key)
                && resUpdate.getValue().equals(updatedValue)
                && resDelete.getKey().equals(key));
    }

    public void testUpdateWithInvalidValue() {
        String key = "foo";
        String initialValue = "bar";
        String invalidValue = "";

        KVMessage resPut = null;
        KVMessage resInvalid = null;
        Exception ex = null;

        try {
            resPut = kvClient.put(key, initialValue);
            resInvalid = kvClient.put(key, invalidValue);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println(resPut.getKey() + ", " + resPut.getValue() + ", " + resPut.getStatus());
        System.out.println(resInvalid.getKey() + ", " + resInvalid.getValue() + ", " + resInvalid.getStatus());
        assertTrue(ex == null && resPut.getStatus() == StatusType.PUT_SUCCESS
                && resInvalid.getStatus() == StatusType.PUT_ERROR
                && resPut.getKey().equals(key)
                && resPut.getValue().equals(initialValue)
                && resInvalid.getKey().equals(key)
                && resInvalid.getValue().equals(invalidValue)); // resInvalid.getValue() should be an empty string, not
                                                                // null
    }

    public void testAccessDeletedValue() {
        String key = "foo";
        String value = "bar";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            kvClient.put(key, "null");
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println(response.getKey() + ", " + response.getValue() + ", " + response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR
                && response.getKey().equals(key));
    }

    public void testGetMultipleValues() {
        KVMessage[] responses = new KVMessage[10];
        Exception ex = null;

        try {
            for (int i = 0; i < responses.length; ++i) {
                String key = "foo" + i;
                String value = "bar" + i;
                kvClient.put(key, value);
            }

            for (int i = 0; i < responses.length; ++i) {
                responses[i] = kvClient.get("foo" + i);
            }

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);

        for (int i = 0; i < responses.length; ++i) {
            assertTrue(responses[i].getStatus() == StatusType.GET_SUCCESS
                    && responses[i].getKey().equals("foo" + i)
                    && responses[i].getValue().equals("bar" + i));
        }
    }

    public void testDeleteMultipleValues() {
        KVMessage[] responses = new KVMessage[10];
        Exception ex = null;

        try {
            for (int i = 0; i < responses.length; ++i) {
                String key = "foo" + i;
                String value = "bar" + i;
                kvClient.put(key, value);
            }

            for (int i = 0; i < responses.length; ++i) {
                responses[i] = kvClient.put("foo" + i, "null");
            }

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null);

        for (int i = 0; i < responses.length; ++i) {
            assertTrue(responses[i].getStatus() == StatusType.DELETE_SUCCESS
                    && responses[i].getKey().equals("foo" + i));
        }
    }
}
