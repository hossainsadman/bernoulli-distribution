package app_kvServer;

import java.io.*;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import server.ClientConnection;

import org.apache.log4j.Logger; // import Logger

public class KVServer implements IKVServer {
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */

	private ServerSocket serverSocket; // Socket IPC
	private int port; // Port number
	private int cacheSize; // Cache size
	private CacheStrategy strategy; // Strategy (given by definition in ./IKVServer.java)
	private boolean running; // Check whether the server is currently running or not
	private Map<String, String> cache = new HashMap<>();
    private static Logger logger = Logger.getRootLogger();
	private final String dirPath;

	public KVServer(int port, int cacheSize, String strategy) {
		// TODO Auto-generated method stub
		this.port = port; // Set port
        this.cacheSize = cacheSize; // Set cache size
        this.strategy = strategy; // Set cache strategy
		dirPath = System.getProperty("user.dir");
	}
	
	@Override
	public int getPort(){
		// TODO Auto-generated method stub
		return port; // Return port
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return InetAddress.getLocalHost().getHostName(); // Return hostname
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return strategy;
	}

	@Override
    public int getCacheSize(){
		// TODO Auto-generated method stub
		return cacheSize; // Return cache size
	}

	private File getStorageAddressOfKey(String key){
		File file = new File(dirPath + File.separator + key);
		return file;
	}

	@Override
    public boolean inStorage(String key){
		// TODO Auto-generated method stub
		File file = getStorageAddressOfKey(key);
		return file.exists();
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}


	@Override
    public String getKV(String key) throws Exception{
		// TODO Auto-generated method stub
		if (!inStorage(key)){
			throw new Exception("[Exception] Key not in storage.");
		} 
		
		String path = getStorageAddressOfKey(key);
		StringBuilder contentBuilder = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(path))){
			String line;
			while ((line = reader.readLine()) != null){
				contentBuilder.append(line).append("\n");
			}
		}

		return contentBuilder.toString().trim();
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		// TODO Auto-generated method stub
		File file = new File(dirPath + File.separator + key);
		if (inStorage(key)){
			throw new Exception("[Exception] Key already in storage.");
		} else {
			try (FileWriter writer = new FileWriter(file)){
				writer.write(value);
			} catch (IOException e){
				logger.error("[Error] Unable to write to file: " + file.getName(), e);
			}
		}
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		// TODO Auto-generated method stub
	}

    private boolean initServer(){
    	try {
            serverSocket = new ServerSocket(port);
			logger.info("[Success] Server is listening on port: " + serverSocket.getLocalPort());    
            return true;
        } catch (IOException e) {
        	logger.error("[Error] Server Socket cannot be opened: ");
            if (e instanceof BindException){
            	logger.error("[Error] Port " + port + " is already bound.");
            }
            return false;
        }
    }

	@Override
    public void run(){
		// TODO Auto-generated method stub
		running = initServer();
        
        if (serverSocket != null){
	        while (running){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = new ClientConnection(client);
	                new Thread(connection).start();
	                logger.info("[Success] Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
	            } catch (IOException e){
	            	logger.error("[Error] Unable to establish connection.\n", e);
	            }
	        }
        }
        logger.info("Server is stopped.");
	}

	@Override
    public void kill(){
		// TODO Auto-generated method stub
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e){
			logger.error("[Error] Unable to close socket on port: " + port, e);
		}
	}

	@Override
    public void close(){
		// TODO Auto-generated method stub
	}
}
