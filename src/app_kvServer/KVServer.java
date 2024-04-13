package app_kvServer;

import java.io.*;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.ServerSocket;
import java.net.Socket;
import logger.LogSetup;
import org.apache.log4j.Logger; // import logger
import org.apache.commons.cli.*;

import shared.messages.KVMessage;
import shared.messages.ECSMessage;
import shared.messages.ECSMessage.ECSMessageType;
import shared.messages.KVMessage.StatusType;
import shared.messages.MessageService;
import shared.*;

import ecs.ECS;
import ecs.ECSHashRing;
import ecs.ECSNode;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class KVServer implements IKVServer {
    /**
     * Start KV Server at given port
     * 
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO",
     *                  "LRU",
     *                  and "LFU".
     */
    private MessageService messageService = new MessageService();
    private ServerSocket serverSocket; // Socket IPC
    private Socket clientSocket;
    private static final List<ClientConnection> connections = new CopyOnWriteArrayList<>();

    private int port; // Port number
    private int cacheSize; // Cache size
    private CacheStrategy strategy; // Strategy (given by definition in ./IKVServer.java)
    private boolean running; // Check whether the server is currently running or not
    private boolean write_lock = false;
    private Caches.Cache<String, String> cache;

    private KVMessage.StatusType status;

    private static Logger logger = Logger.getRootLogger();

    private final String dirPath;
    private String ecsHost = null;
    private int ecsPort = -1;
    private Socket ecsSocket;
    private Boolean connectEcs = true; // for testing purposes

    /* Meta Data */
    private ECSHashRing hashRing = null;
    private ECSNode metadata = null;

    private ObjectInputStream ecsInStream;
    private ObjectOutputStream ecsOutStream;

    private Replicator replicator;

    private Map<String, SQLTable> sqlTables;
    private static Gson gson = new Gson();
    private static JsonParser jsonParser = new JsonParser();

    public KVServer(int port, int cacheSize, String strategy, Boolean connectEcs) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");
        if (cacheSize < 0)
            throw new IllegalArgumentException("cacheSize is out of range.");

        this.port = port; // Set port
        this.cacheSize = cacheSize; // Set cache size
        this.status = KVMessage.StatusType.SERVER_ACTIVE;
        this.ecsHost = ECS.getDefaultECSAddr();
        this.ecsPort = ECS.getDefaultECSPort();
        this.connectEcs = connectEcs;
        this.replicator = new Replicator(this);
        this.sqlTables = new HashMap<>();
        
        if (strategy == null) {
            this.strategy = CacheStrategy.None;
            this.cache = null;
        } else {
            switch (strategy) { // Set cache strategy
                case "LRU":
                    this.strategy = CacheStrategy.LRU;
                    this.cache = new Caches.LRUCache(this.cacheSize);
                    break;
                case "LFU":
                    this.strategy = CacheStrategy.LFU;
                    this.cache = new Caches.LFUCache(this.cacheSize);
                    break;
                case "FIFO":
                    this.strategy = CacheStrategy.FIFO;
                    this.cache = new Caches.FIFOCache(this.cacheSize);
                    break;
                default:
                    this.strategy = CacheStrategy.None;
                    this.cache = null;
            }
        }

        dirPath = System.getProperty("user.dir") + File.separator + "db";
        File dir = new File(dirPath);
        if (!dir.exists()) {
            try {
                if (!dir.mkdir()) {
                    throw new Exception("Unable to create a directory.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                KVServer.this.run();
            }
        });

        serverThread.start(); // Start the thread
    }

    public KVServer(int port, int cacheSize, String strategy, String dbPath, Boolean connectEcs) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");
        if (cacheSize < 0)
            throw new IllegalArgumentException("cacheSize is out of range.");

        this.port = port; // Set port
        this.cacheSize = cacheSize; // Set cache size
        this.status = KVMessage.StatusType.SERVER_ACTIVE;
        this.connectEcs = connectEcs;
        this.replicator = new Replicator(this);
        this.sqlTables = new HashMap<>();

        if (strategy == null) {
            this.strategy = CacheStrategy.None;
            this.cache = null;
        } else {
            switch (strategy) { // Set cache strategy
                case "LRU":
                    this.strategy = CacheStrategy.LRU;
                    this.cache = new Caches.LRUCache(this.cacheSize);
                    break;
                case "LFU":
                    this.strategy = CacheStrategy.LFU;
                    this.cache = new Caches.LFUCache(this.cacheSize);
                    break;
                case "FIFO":
                    this.strategy = CacheStrategy.FIFO;
                    this.cache = new Caches.FIFOCache(this.cacheSize);
                    break;
                default:
                    this.strategy = CacheStrategy.None;
                    this.cache = null;
            }
        }

        dirPath = System.getProperty("user.dir") + File.separator + dbPath;
        File dir = new File(dirPath);
        if (!dir.exists()) {
            try {
                if (!dir.mkdir()) {
                    throw new Exception("Unable to create a directory.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                KVServer.this.run();
            }
        });

        serverThread.start(); // Start the thread
    }

    public KVServer(int port, int cacheSize, String strategy, String dbPath, String ecsHost, int ecsPort) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");
        if (cacheSize < 0)
            throw new IllegalArgumentException("cacheSize is out of range.");

        this.port = port; // Set port
        this.cacheSize = cacheSize; // Set cache size
        this.status = KVMessage.StatusType.SERVER_ACTIVE;
        this.ecsHost = ecsHost;
        this.ecsPort = ecsPort;
        this.replicator = new Replicator(this);
        this.sqlTables = new HashMap<>();

        if (strategy == null) {
            this.strategy = CacheStrategy.None;
            this.cache = null;
        } else {
            switch (strategy) { // Set cache strategy
                case "LRU":
                    this.strategy = CacheStrategy.LRU;
                    this.cache = new Caches.LRUCache(this.cacheSize);
                    break;
                case "LFU":
                    this.strategy = CacheStrategy.LFU;
                    this.cache = new Caches.LFUCache(this.cacheSize);
                    break;
                case "FIFO":
                    this.strategy = CacheStrategy.FIFO;
                    this.cache = new Caches.FIFOCache(this.cacheSize);
                    break;
                default:
                    this.strategy = CacheStrategy.None;
                    this.cache = null;
            }
        }

        dirPath = System.getProperty("user.dir") + File.separator + dbPath;
        File dir = new File(dirPath);
        if (!dir.exists()) {
            try {
                if (!dir.mkdir()) {
                    throw new Exception("Unable to create a directory.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                KVServer.this.run();
            }
        });

        serverThread.start(); // Start the thread
    }

    public static boolean checkValidJson(String str) {
        try {
            jsonParser.parse(str);
            return true;
        } catch (JsonSyntaxException e) {
            return false;
        }
    }

    public SQLTable createSQLTable(String tableName, String primaryKey, Map<String, String> cols) {
        SQLTable newTable = new SQLTable(tableName, primaryKey);
        for (Map.Entry<String, String> entry : cols.entrySet()) {
            String colName = entry.getKey();
            String colType = entry.getValue();
            Class<?> typeClass;
            if (colType.equals("int")) {
                typeClass = Integer.class;
            } else if (colType.equals("text")) {
                typeClass = String.class;
            } else {
                throw new IllegalArgumentException("Invalid type for column " + colName + ": " + colType);
            }
            newTable.addCol(colName, typeClass);
        }
        return newTable;
    }

    public void removeKeys(){
        File dir = new File(dirPath);
        File[] db = dir.listFiles();
        for (File kv : db) {
            if(isCoordinatorOrReplicator(kv.getName())) continue;
            System.out.println("Deleting " + kv.getName());
            kv.delete();
            cache.remove(unescape(kv.getName()));
        }
    }

    public void moveKeys() throws Exception{
        File dir = new File(dirPath);
        File[] db = dir.listFiles();
        for (File kv : db) {
            String key = kv.getName();
            if(isCoordinator(key)) {
                System.out.println("Moving " + key);
                this.replicate(unescape(key), getKV(unescape(key)));
            }
        }
    }

    public void setHashRing(ECSHashRing newHashRing) throws Exception{
        BigInteger prevStartHash = null, prevEndHash = null;
        ECSNode newNode = newHashRing.getNodeForIdentifier(this.getStringIdentifier());
        
        BigInteger startHash = newNode.getNodeHashStartRange();
        BigInteger endHash = newNode.getNodeHashEndRange();

        if (this.hashRing != null){
            ECSNode prevNode = this.hashRing.getNodeForIdentifier(this.getStringIdentifier());
            prevStartHash = prevNode.getNodeHashStartRange();
            prevEndHash = prevNode.getNodeHashEndRange();
        }
        HashMap<BigInteger, HashMap<String, String>> serverKvPairs = new HashMap<>();

        if (prevStartHash != null && prevEndHash != null){
            File dir = new File(dirPath);
            File[] db = dir.listFiles();
            for (File kv : db) {
                String key = kv.getName();
                if (isCoordinator(key) && ECSNode.isKeyInRange(key, prevStartHash, prevEndHash) && !ECSNode.isKeyInRange(key, startHash, endHash)){
                    BigInteger bigIntegerKey = newHashRing.getNodeForKey(key).getNodeIdentifier();
                    serverKvPairs.computeIfAbsent(bigIntegerKey, k -> new HashMap<>()).put(unescape(key), getKV(unescape(key)));
                }
            }

            if (serverKvPairs.size() > 0){
                for (Map.Entry<BigInteger, HashMap<String, String>> entry : serverKvPairs.entrySet()) {
                    ECSNode toNode = newHashRing.getNodeForIdentifier(entry.getKey());
                    HashMap<String, String> kvPairs = entry.getValue();
                    messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.TRANSFER_TO, "TO_NODE", toNode, "KV_PAIRS", kvPairs);
                }
            }
        }

        System.out.println("setting hash ring");
        this.hashRing = newHashRing;
        this.replicator.connect(newHashRing);
        this.removeKeys();

        try {
            this.moveKeys();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error occured when moving keys");
        }
    }

    public ECSHashRing getHashRing() {
        return hashRing;
    }

    public void setMetadata(ECSNode metadata) {
        this.metadata = metadata;
    }

    public ECSNode getMetadata() {
        return metadata;
    }

    public boolean replicate(String key, String value){
        try {
            return this.replicator.replicate(key, value);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error occured when replicating");
        }
        return false;
    }

    public boolean isCoordinator(String key){
        return this.getMetadata().isKeyInRange(key);
    }

    public boolean isReplicator(String key){
        ECSNode[] replicaNodes = this.getHashRing().getPrevTwoPredecessors(this.getMetadata());
        if (replicaNodes[0] != null){
            if (replicaNodes[0].isKeyInRange(key)) return true;
        }
        if (replicaNodes[1] != null){
            if (replicaNodes[1].isKeyInRange(key)) return true;
        }
        return false;
    }

    public boolean isCoordinatorOrReplicator(String key){
        return this.isCoordinator(key) || this.isReplicator(key);
    }

    public String getStringIdentifier(){
        return getHostaddress() + ":" + String.valueOf(this.getPort());
    }

    @Override
    public int getPort() {
        return port; // Return port
    }

    public static String getHostaddress() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostAddress(); // Return hostname
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "Unknown Host";
        }
    }

    @Override
    public String getHostname() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostName(); // Return hostname
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "Unknown Host";
        }
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return strategy; // Return strategy
    }

    @Override
    public int getCacheSize() {
        return cacheSize; // Return cache size
    }

    private File getStorageAddressOfKey(String key) {
        File file = new File(dirPath + File.separator + key);
        return file;
    }

    @Override
    public boolean inStorage(String key) {
        File file = getStorageAddressOfKey(key);
        return file.exists();
    }

    @Override
    public boolean inCache(String key) {
        return cache != null && cache.containsKey(key);
    }

    public static String escape(String s) {
        return s.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\b", "\\b")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\f", "\\f")
                .replace("\'", "\\'")
                .replace("\"", "\\\"")
                .replace(" ", "\\ ");
    }

    public static String unescape(String s) {
        return s.replace("\\\\", "\\")
                .replace("\\t", "\t")
                .replace("\\b", "\b")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\f", "\f")
                .replace("\\'", "'")
                .replace("\\\"", "\"")
                .replace("\\ ", " ");
    }

    @Override
    public String getKV(String key) throws Exception {
        if (!inStorage(escape(key)))
            throw new Exception("tuple not found");

        if (!inCache(escape(key))) {
            File path = getStorageAddressOfKey(escape(key));
            StringBuilder contentBuilder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = reader.readLine()) != null)
                    contentBuilder.append(line).append("\n");
            }

            return contentBuilder.toString().trim();
        }

        String value = cache.get(escape(key));
        if (value == null)
            throw new Exception("tuple not found");

        return value;
    }

    @Override
    public synchronized StatusType putKV(String key, String value) throws Exception {
        if (write_lock) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception("unable to delete tuple");

        File file = new File(dirPath + File.separator + escape(key));

        if (value.equals("null")) {
            File fileToDel = new File(dirPath, escape(key));
            if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
                throw new Exception("unable to delete tuple");

            cache.remove(escape(key));

            return StatusType.DELETE_SUCCESS;
        }

        if (inStorage(escape(key))) { // Key is already in storage (i.e. UPDATE)
            try (FileWriter writer = new FileWriter(file, false)) { // overwrite
                writer.write(value);
                if (this.cache != null)
                    cache.put(escape(key), value);
            }

            return StatusType.PUT_UPDATE;
        }

        // Key is not in storage (i.e. PUT)
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(value);
            if (this.cache != null)
                cache.put(escape(key), value);
        }
        return StatusType.PUT_SUCCESS;
    }

    public synchronized StatusType putKV(String key, String value, boolean override) throws Exception {
        if (write_lock & !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception("unable to delete tuple");

        File file = new File(dirPath + File.separator + escape(key));

        if (value.equals("null")) {
            File fileToDel = new File(dirPath, escape(key));
            if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
                throw new Exception("unable to delete tuple");

            cache.remove(escape(key));

            return StatusType.DELETE_SUCCESS;
        }

        if (inStorage(escape(key))) { // Key is already in storage (i.e. UPDATE)
            try (FileWriter writer = new FileWriter(file, false)) { // overwrite
                writer.write(value);
                if (this.cache != null)
                    cache.put(escape(key), value);
            }

            return StatusType.PUT_UPDATE;
        }

        // Key is not in storage (i.e. PUT)
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(value);
            if (this.cache != null)
                cache.put(escape(key), value);
        }
        return StatusType.PUT_SUCCESS;
    }

    public synchronized StatusType sqlCreate(String key, String value) throws Exception {
        if (write_lock) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception("empty sql create value");

        // if (value.equals("null")) {
        //     File fileToDel = new File(dirPath, escape(key));
        //     if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
        //         throw new Exception("unable to delete tuple");

        //     cache.remove(escape(key));

        //     return StatusType.DELETE_SUCCESS;
        // }

        // if (inStorage(escape(key))) { // Key is already in storage (i.e. UPDATE)
        //     try (FileWriter writer = new FileWriter(file, false)) { // overwrite
        //         writer.write(value);
        //         if (this.cache != null)
        //             cache.put(escape(key), value);
        //     }

        //     return StatusType.PUT_UPDATE;
        // }

        // // Key is not in storage (i.e. PUT)
        // try (FileWriter writer = new FileWriter(file)) {
        //     writer.write(value);
        //     if (this.cache != null)
        //         cache.put(escape(key), value);
        // }
        // return StatusType.PUT_SUCCESS;

        if (sqlTables.containsKey(key)) {
            throw new Exception("A table with the same name already exists");
        }

        boolean validSqlCreate = true;
        String[] colPairs = value.split(",");
        Map<String, String> cols = new HashMap<>();
        for (String pair : colPairs) {
            String[] parts = pair.split(":");
            if (parts.length != 2) {
                this.logger.error("Invalid column pair: " + pair);
                validSqlCreate = false;
                break;
            }
            String name = parts[0];
            String type = parts[1];
            if (!type.equals("int") && !type.equals("text")) {
                this.logger.error("Invalid type for column " + name + ": " + type);
                validSqlCreate = false;
                break;
            }
            if (cols.containsKey(name)) {
                this.logger.error("Column name " + name + " is repeated");
                validSqlCreate = false;
                break;
            }
            cols.put(name, type);
        }

        if (!validSqlCreate) {
            return StatusType.SQLCREATE_ERROR;
        }

        if (validSqlCreate) {
            for (Map.Entry<String, String> entry : cols.entrySet()) {
                String name = entry.getKey();
                String type = entry.getValue();
                logger.info("Column name: " + name + ", Type: " + type);
            }
        }

        String primaryKey = cols.keySet().iterator().next();
        SQLTable table = createSQLTable(key, primaryKey, cols);
        sqlTables.put(key, table);

        // Print out the table
        this.logger.info(table.toString());

        return StatusType.SQLCREATE_SUCCESS;
    }

    public String sqlSelect(String key) throws Exception {
        if (!sqlTables.containsKey(key)) {
            throw new Exception("table not found");
        }

        SQLTable table = sqlTables.get(key);
        return table.toString();
    }

    public String sqlDrop(String key) throws Exception {
        if (!sqlTables.containsKey(key)) {
            throw new Exception("table not found");
        }

        sqlTables.remove(key);
        return key + " dropped";
    }
    
    public synchronized StatusType sqlInsert(String key, String value) throws Exception {
        if (write_lock) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception("empty sql insert value");

        // if (value.equals("null")) {
        //     File fileToDel = new File(dirPath, escape(key));
        //     if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
        //         throw new Exception("unable to delete tuple");

        //     cache.remove(escape(key));

        //     return StatusType.DELETE_SUCCESS;
        // }

        // if (inStorage(escape(key))) { // Key is already in storage (i.e. UPDATE)
        //     try (FileWriter writer = new FileWriter(file, false)) { // overwrite
        //         writer.write(value);
        //         if (this.cache != null)
        //             cache.put(escape(key), value);
        //     }

        //     return StatusType.PUT_UPDATE;
        // }

        // // Key is not in storage (i.e. PUT)
        // try (FileWriter writer = new FileWriter(file)) {
        //     writer.write(value);
        //     if (this.cache != null)
        //         cache.put(escape(key), value);
        // }
        // return StatusType.PUT_SUCCESS;

        if (!sqlTables.containsKey(key)) {
            this.logger.error("table does not exist");
            throw new Exception("table does not exist");
        }

        if (!checkValidJson(value)) {
            this.logger.error("table row has invalid formatting");
            throw new Exception("table row has invalid formatting");
        }

        SQLTable table = sqlTables.get(key);
        Map<String, Object> rowMap = new HashMap<>();
        
        JsonElement jsonElement = null;
        try {
            jsonElement = jsonParser.parse(value);
        } catch (JsonParseException e) {
            this.logger.error("Invalid JSON format: " + e.getMessage());
        }

        try {
            if (jsonElement != null && jsonElement.isJsonObject()) {
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                for (String jsonKey : jsonObject.keySet()) {
                    try {
                        JsonElement elem = jsonObject.get(jsonKey);
                        if (table.cols.contains(jsonKey)) {
                            if (table.colTypes.get(jsonKey).equals(Integer.class)) {
                                try {
                                    String colType = table.colTypes.get(table.cols.indexOf(jsonKey)).toString();
                                    String colName = jsonKey;
                                    String colValue = elem.getAsString();
                                    this.logger.info("Column Type: " + colType);
                                    this.logger.info("Column Name: " + colName);
                                    this.logger.info("Column Value: " + colValue);
                                    int intValue = Integer.parseInt(elem.getAsString());
                                    rowMap.put(jsonKey, intValue);
                                } catch (NumberFormatException e) {
                                    this.logger.error("Error: Value for column " + jsonKey + " must be an integer");
                                    return StatusType.SQLINSERT_ERROR;
                                }
                            } else {
                                rowMap.put(jsonKey, elem.getAsString());
                            }
                        } else {
                            this.logger.error(jsonKey + " is not a column in table" + key);
                            return StatusType.SQLINSERT_ERROR;
                        }
                    } catch (Exception e) {
                        this.logger.error(e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            this.logger.error("Error adding row to table: " + e.getMessage());
            return StatusType.SQLINSERT_ERROR;
        }

        JsonObject jsonObject = jsonElement.getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String columnName = entry.getKey();
            JsonElement columnValue = entry.getValue();
            if (columnValue.isJsonPrimitive()) {
                rowMap.put(columnName, columnValue.getAsString());
            } else {
                rowMap.put(columnName, columnValue.toString());
            }
        }

        try {
            table.addRow(rowMap);
        } catch (Exception e) {
            this.logger.error("Error adding row to table: " + e.getMessage());
            return StatusType.SQLINSERT_ERROR;
        }

        this.logger.info(table.toString());
        return StatusType.SQLINSERT_SUCCESS;
    }

    /*
     * Helper Function to Monitor the State of Current KVs in Storage and Cache
     */
    public void printStorageAndCache() {
        File dir = new File(dirPath);
        File[] db = dir.listFiles();

        System.out.println("Storage: ");
        if (db == null || db.length == 0)
            System.out.println("\tStorage is Empty.");
        else {
            for (File kv : db) {
                System.out.print("\t" + "Key: " + kv.getName() + ", "); // key
                try {
                    String content = new String(Files.readAllBytes(kv.toPath()));
                    System.out.println("Value: " + content); // value
                } catch (IOException e) {
                    System.out.println("<Error>"); // could not access value for whatever reason
                }
            }
        }

        System.out.println("Cache: ");
        if (cache == null || cache.size() == 0)
            System.out.println("\tCache is Empty.");
        for (Map.Entry<String, String> kv : cache.entrySet())
            System.out.println("\t" + "Key: " + kv.getKey() + ", Value: " + kv.getValue());

        for (int i = 0; i < 40; ++i) // Divider for readability
            System.out.print("-");
        System.out.println();
    }

    @Override
    public void clearCache() {
        if (cache == null)
            return;

        cache.removeAll();
    }

    @Override
    public void clearStorage() {
        File dir = new File(dirPath);
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                for (File kv : file.listFiles())
                    kv.delete();
            }
            file.delete();
        }
    }

    @SuppressWarnings("unchecked")
    private void listenToEcsSocket() throws Exception{
        HashMap<String, String> kvPairs = new HashMap<>();
        while (ecsSocket != null && !ecsSocket.isClosed() && running) {
            ECSMessage message = messageService.receiveECSMessage(ecsSocket, this.ecsInStream);
            if (message == null) {
                // Connection has been closed by ECS, handle gracefully
                System.out.println("Socket connection closed, stopping listener.");
                break;
            }

            switch (message.getType()){
                case HASHRING: {
                    System.out.println("RECEIVED HASHRING COMMAND");
                    this.setHashRing((ECSHashRing) message.getParameter("HASHRING"));
                    System.out.println(hashRing.toString());


                    if(metadata != null) this.logger.info("Old hashrange: " + metadata.toString());
                    setMetadata(hashRing.getNodeForIdentifier(getHostaddress() + ":" + String.valueOf(this.getPort())));
                    if(metadata != null) this.logger.info("Up to date hashrange: " + metadata.toString());
                    break;
                }

                case TRANSFER_FROM:{
                    this.logger.info("Received TRANSFER_FROM command from ECS");
                    ECSNode toNode = (ECSNode) message.getParameter("TO_NODE");
                    // get keys to transfer
                    
                    try{
                        kvPairs = getKVPairsNotResponsibleFor();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    // transfer keys
                    if (kvPairs != null && kvPairs.size() > 0){
                        messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.TRANSFER_TO, "TO_NODE", toNode, "KV_PAIRS", kvPairs);
                    }

                    break;
                }
                
                case RECEIVE:{
                    this.logger.info("Received RECIEVE command from ECS");
                    ECSNode fromNode = (ECSNode) message.getParameter("FROM_NODE");
                    
                    this.write_lock = true;
                    
                    kvPairs = (HashMap<String, String>) message.getParameter("KV_PAIRS");
                    System.out.println(kvPairs);
                    for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                        try {
                            StatusType putStatus = putKV(entry.getKey(), entry.getValue(), true);
                            if (putStatus != StatusType.SERVER_WRITE_LOCK){
                                if (this.replicate(entry.getKey(), entry.getValue())){
                                    this.logger.info("Replication success");
                                } else {
                                    this.logger.info("Replication failure");
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    
                    this.write_lock = false;
                    if(fromNode != null)
                        messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.TRANSFER_COMPLETE, "PING_NODE", fromNode);

                    break;
                }

                case TRANSFER_COMPLETE:{
                    this.logger.info("Received TRANSFER_COMPLETE command from ECS");
                    for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                        try {
                            putKV(entry.getKey(), "null");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                }

                case SHUTDOWN_SERVER: {
                    System.out.println("Received SHUTDOWN_SERVER command");
                    this.close();
                    break;
                }

                default:
                    System.out.println("Unrecognized Command in KVSERVER ");
            }
        }
    }

    public void connectECS() {
        if (ecsHost != null && ecsPort > -1) {
            try {
                ecsSocket = new Socket(ecsHost, ecsPort);
                this.logger.info("Connected to ECS at " + ecsHost + ":" + ecsPort + " via "
                        + ecsSocket.getInetAddress().getHostAddress()
                        + ":" + ecsSocket.getLocalPort());


                String serverName = getHostaddress() + ":" + String.valueOf(this.getPort());
                System.out.println(serverName);
                ecsOutStream = new ObjectOutputStream(ecsSocket.getOutputStream());
                ecsOutStream.flush();
                ecsInStream = new ObjectInputStream(ecsSocket.getInputStream());

                messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.INIT, "SERVER_NAME", serverName);

                new Thread(new Runnable() {
                    public void run() {
                        try {
                            listenToEcsSocket();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("[KVServer] ECS Disconnected.");
                        }
                    }
                }).start();

            } catch (Exception e) {
                System.err.println("Error connecting to ECS");
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        running = true;
        try {
            serverSocket = new ServerSocket(port);
            if (ecsHost != null && ecsPort >= 0)
                this.logger.info("Started server listening at: " + "(" + serverSocket.getInetAddress().getHostName() + ") "
                        + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort()
                        + "; cache size: "
                        + cacheSize + "; cache strategy: " + strategy + "; ECS set to: " + ecsHost + ":" + ecsPort);

        } catch (IOException e) {
            this.logger.error("Server Socket cannot be opened: ");
            if (e instanceof BindException)
                this.logger.error("Port " + port + " is already bound.");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.shutdownHook();
        }));

        if (this.connectEcs){
            connectECS();
        }

        if (serverSocket != null) {
            while (running) {
                try {
                    clientSocket = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(this, clientSocket);
                    connections.add(connection);
                    new Thread(connection).start();
                    this.logger.info("Connected to " + "(" + clientSocket.getInetAddress().getHostName() + ") "
                            + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort());
                } catch (IOException e) {
                    this.logger.error("Unable to establish connection.\n", e);
                }
            }
        }
        this.logger.info("Server is stopped.");
        close();
    }

    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            this.logger.error("Unable to close socket on port: " + port, e);
        }

    }

    @Override
    public void close() {
        running = false;
        for (ClientConnection conn : connections)
            conn.close();
        clearCache();
        // clearStorage(); // are not supposed to clear storage on server start/quit
        kill();
    }

    public void shutdownHook(){
        HashMap<String, String> kvPairs = null;
        System.out.println("Running shutdown hook");
        try {
            kvPairs = getAllKVPairs();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if(this.hashRing != null && this.hashRing.getHashring().size() > 1 && this.ecsSocket != null){
                messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.SHUTDOWN, "KV_PAIRS", kvPairs);
                for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                    putKV(entry.getKey(), "null");
                }
                File index = new File(dirPath);
                index.delete();
            } else {
                System.out.println("Last server, not transferring KV Pairs");
            }
        } catch (Exception e) {
            System.out.println("GOT AN ERRROR");
            e.printStackTrace();
        }
        try {
            if (this.ecsSocket != null)
                ecsSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, String> getKVPairsNotResponsibleFor() throws Exception {
        HashMap<String, String> kvPairs = new HashMap<>();
        File dir = new File(dirPath);
        if (dir.isDirectory()) {
            for (File kv : dir.listFiles()) {
                String key = kv.getName();
                if (!metadata.isKeyInRange(key)) {
                    kvPairs.put(unescape(key), getKV(unescape(key)));
                }
            }
        }
        this.logger.info("KVPairs not responsible for: " + kvPairs.toString());
        return kvPairs;
    }

    public HashMap<String, String> getAllKvPairsResponsibleFor() throws Exception {
        HashMap<String, String> kvPairs = new HashMap<>();
        File dir = new File(dirPath);
        if (dir.isDirectory()) {
            for (File kv : dir.listFiles()) {
                String key = kv.getName();
                if (metadata.isKeyInRange(key)) {
                    kvPairs.put(unescape(key), getKV(unescape(key)));
                }
            }
        }
        return kvPairs;
    }

    public HashMap<String, String> getAllKVPairs() throws Exception { 
        HashMap<String, String> kvPairs = new HashMap<>();
        File dir = new File(dirPath);
        if (dir.isDirectory()) {
            for (File kv : dir.listFiles()) {
                String key = kv.getName();
                kvPairs.put(unescape(key), getKV(unescape(key)));
            }
        }
        this.logger.info("KVPairs responsible for: " + kvPairs.toString());
        return kvPairs;
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();

        Option help = new Option("h", "help", false, "display help");
        help.setRequired(false);
        options.addOption(help);

        Option address = new Option("a", "address", true, "address to listen to");
        address.setRequired(false);
        options.addOption(address);

        Option port = new Option("p", "port", true, "server port");
        port.setRequired(false);
        options.addOption(port);

        Option cacheSize = new Option("c", "cacheSize", true, "cache size");
        cacheSize.setRequired(false);
        options.addOption(cacheSize);

        Option cacheStrategy = new Option("s", "cacheStrategy", true, "cache strategy");
        cacheStrategy.setRequired(false);
        options.addOption(cacheStrategy);

        Option logFile = new Option("l", "logFile", true, "log file path");
        logFile.setRequired(false);
        options.addOption(logFile);

        Option logLevel = new Option("ll", "logLevel", true, "log level");
        logLevel.setRequired(false);
        options.addOption(logLevel);

        Option dataPath = new Option("d", "dir", true, "directory to persist data");
        dataPath.setRequired(false);
        options.addOption(dataPath);

        Option ecsHostAndPort = new Option("b", "ecsHostAndPort", true, "ecs host and port");
        ecsHostAndPort.setRequired(false);
        options.addOption(ecsHostAndPort);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("m4-server.jar", options);
            System.exit(0);
        }

        // set defaults for options
        String serverAddress = cmd.getOptionValue("address", "localhost");
        String serverPort = (cmd.getOptionValue("port", "20010"));
        String serverCacheSize = (cmd.getOptionValue("cacheSize", "10"));
        String serverCacheStrategy = (cmd.getOptionValue("cacheStrategy", "FIFO"));
        String serverLogFile = cmd.getOptionValue("logFile", "logs/server.log");
        String serverLogLevel = cmd.getOptionValue("logLevel", "ALL");

        String dbPath = cmd.getOptionValue("dir", "db" + MD5.getHash(getHostaddress() + ":" + serverPort));
        String ecsHostAndPortString = cmd.getOptionValue("ecsHostAndPort", null);

        if (!LogSetup.isValidLevel(serverLogLevel)) {
            serverLogLevel = "ALL";
        }

        String ecsHostCli = null;
        int ecsPortCli = -1;
        if (ecsHostAndPortString != null) {
            String[] parts = ecsHostAndPortString.split(":");
            ecsHostCli = parts[0];
            ecsPortCli = Integer.parseInt(parts[1]);
        } else {
            ecsHostCli = ECS.getDefaultECSAddr();
            ecsPortCli = ECS.getDefaultECSPort();
        }

        try {
            new LogSetup(serverLogFile, LogSetup.getLogLevel(serverLogLevel));
            KVServer server;

            server = new KVServer(Integer.parseInt(serverPort), Integer.parseInt(serverCacheSize), serverCacheStrategy,
                    dbPath, ecsHostCli, ecsPortCli);
            // server.clearStorage(); // are not supposed to clear storage
            // on server start/quit
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}