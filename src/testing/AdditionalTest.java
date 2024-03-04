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

        assertTrue(ex == null && resUpdate.getStatus() == StatusType.PUT_UPDATE
                && resDelete.getStatus() == StatusType.DELETE_SUCCESS
                && resUpdate.getKey().equals(key)
                && resUpdate.getValue().equals(updatedValue)
                && resDelete.getKey().equals(key));
    }

    public void testAccessDeletedValue() {
        String key = "accessDeleted";
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

    /* KeyValueTests */
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
        String key = "key\nwith\nnewlines1";
        String value = "value\nwith\nnewlines";
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
