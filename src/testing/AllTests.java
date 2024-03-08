package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import ecs.ECS;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import shared.MD5;


public class AllTests {
	static KVServer server;

	static {
		try {
			String dbPath =  "db" + MD5.getHash(KVServer.getHostaddress() + ":" + 50000);
			String ecsHostCli = ECS.getDefaultECSAddr();
            int ecsPortCli = ECS.getDefaultECSPort();
			new LogSetup("logs/testing/test.log", Level.ERROR);
			server = new KVServer(50000, 10, "FIFO", dbPath, ecsHostCli, ecsPortCli);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static KVServer getServer() {
		return server;
	}	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(HashingTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		clientSuite.addTestSuite(CacheTest.class); 
		clientSuite.addTestSuite(M2Test.class); 
		return clientSuite;
	}
	
}
