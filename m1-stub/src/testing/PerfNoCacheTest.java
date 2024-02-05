package testing;

import org.junit.Test;

import java.util.Random;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.KVServer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PerfNoCacheTest extends TestCase {
	
	private int NUM_REQUESTS = 50;
	private double PERCENTAGE_PUTS = 0.5;
	private double PERCENTAGE_GETS = 0.5;
	
	private int NUM_PUTS = 0;
	private int NUM_GETS = 0;
	
	long startTime;
	long endTime;
	long elapsedTime;
	
    private int CACHE_SIZE = 0;
	private String CACHE_STRATEGY = "None";
	
	private String ADDRESS = "localhost";
	private int PORT = 9999;
	
	Random rand = new Random();

	private static Logger logger = Logger.getRootLogger();

	private KVStore kvStore;
	private KVServer server;

    public void setUp() {
		server = new KVServer(PORT, CACHE_SIZE, CACHE_STRATEGY);
		kvStore = new KVStore(ADDRESS, PORT);
		try {
			kvStore.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvStore.disconnect();
        server.kill();
	}

	public void logStats() {
		logger.warn("\n--- Test statistics ---");
		logger.warn("Percentage of PUTS: " + PERCENTAGE_PUTS * 100 + "%");
		logger.warn("Percentage of GETS: " + PERCENTAGE_GETS * 100 + "%");
		logger.warn("Number of PUTS: " + NUM_PUTS);
		logger.warn("Number of GETS: " + NUM_GETS);
		logger.warn("Elapsed time: " + elapsedTime + " ms");

        // -------------------------------------------------

		PERCENTAGE_PUTS = 1;
		PERCENTAGE_GETS = 0;

		NUM_PUTS = (int) (NUM_REQUESTS * PERCENTAGE_PUTS);;
		NUM_GETS = (int) (NUM_REQUESTS * PERCENTAGE_GETS);;

		response = null;
		ex = null;

		startTime = System.nanoTime();

		for (int i = 0; i < NUM_PUTS; i++) {
			try {
				response = kvStore.put(Integer.toString(rand.nextInt(NUM_PUTS)), 
									   Integer.toString(rand.nextInt(NUM_PUTS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		for (int i = 0; i < NUM_GETS; i++) {
			try {
				response = kvStore.get(Integer.toString(rand.nextInt(NUM_GETS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		endTime = System.nanoTime();
		elapsedTime = (endTime - startTime) /  1_000_000; // in milliseconds

		logStats();

		assertNull(ex);
		

		server.clearCache();
		server.clearStorage();
	}

	@Test
	public void testPut80Get20() throws InterruptedException {
		PERCENTAGE_PUTS = 0.8;
		PERCENTAGE_GETS = 0.2;

		NUM_PUTS = (int) (NUM_REQUESTS * PERCENTAGE_PUTS);;
		NUM_GETS = (int) (NUM_REQUESTS * PERCENTAGE_GETS);;

		response = null;
		ex = null;

		startTime = System.nanoTime();

		for (int i = 0; i < NUM_PUTS; i++) {
			try {
				response = kvStore.put(Integer.toString(rand.nextInt(NUM_PUTS)), 
									   Integer.toString(rand.nextInt(NUM_PUTS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		for (int i = 0; i < NUM_GETS; i++) {
			try {
				response = kvStore.get(Integer.toString(rand.nextInt(NUM_GETS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		endTime = System.nanoTime();
		elapsedTime = (endTime - startTime) /  1_000_000; // in milliseconds

		logStats();

		assertNull(ex);
		

		server.clearCache();
		server.clearStorage();
        
        // -------------------------------------------------

		PERCENTAGE_PUTS = 0.5;
		PERCENTAGE_GETS = 0.5;

		NUM_PUTS = (int) (NUM_REQUESTS * PERCENTAGE_PUTS);;
		NUM_GETS = (int) (NUM_REQUESTS * PERCENTAGE_GETS);;

		response = null;
		ex = null;

		startTime = System.nanoTime();

		for (int i = 0; i < NUM_PUTS; i++) {
			try {
				response = kvStore.put(Integer.toString(rand.nextInt(NUM_PUTS)), 
									   Integer.toString(rand.nextInt(NUM_PUTS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		for (int i = 0; i < NUM_GETS; i++) {
			try {
				response = kvStore.get(Integer.toString(rand.nextInt(NUM_GETS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		endTime = System.nanoTime();
		elapsedTime = (endTime - startTime) /  1_000_000; // in milliseconds

		logStats();

		assertNull(ex);
		

		server.clearCache();
		server.clearStorage();

        // -------------------------------------------------

		PERCENTAGE_PUTS = 0.2;
		PERCENTAGE_GETS = 0.8;

		NUM_PUTS = (int) (NUM_REQUESTS * PERCENTAGE_PUTS);;
		NUM_GETS = (int) (NUM_REQUESTS * PERCENTAGE_GETS);;

		response = null;
		ex = null;

		startTime = System.nanoTime();

		for (int i = 0; i < NUM_PUTS; i++) {
			try {
				response = kvStore.put(Integer.toString(rand.nextInt(NUM_PUTS)), 
									   Integer.toString(rand.nextInt(NUM_PUTS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		for (int i = 0; i < NUM_GETS; i++) {
			try {
				response = kvStore.get(Integer.toString(rand.nextInt(NUM_GETS)));
			} catch (Exception e) {
				ex = e;
			}
		}

		endTime = System.nanoTime();
		elapsedTime = (endTime - startTime) /  1_000_000; // in milliseconds

		logStats();

		assertNull(ex);
		

		server.clearCache();
		server.clearStorage();
	}
}
