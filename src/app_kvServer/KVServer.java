package app_kvServer;

import java.io.*;
import java.math.BigInteger;
import java.net.BindException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashMap;

import java.util.ArrayList;
import java.util.Arrays;
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

import app_kvServer.SQLTable;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import shared.ConsoleColors;

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

    private HashMap<String, SQLTable> sqlTables;
    private static Gson gson = new Gson();
    private static JsonParser jsonParser = new JsonParser();

    public KVServer(int port, int cacheSize, String strategy, Boolean connectEcs) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "port is out of range." + ConsoleColors.RESET);
        if (cacheSize < 0)
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "cacheSize is out of range." + ConsoleColors.RESET);

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
                    throw new Exception(ConsoleColors.RED_UNDERLINED + "Unable to create a directory." + ConsoleColors.RESET);
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
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "port is out of range." + ConsoleColors.RESET);
        if (cacheSize < 0)
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "cacheSize is out of range." + ConsoleColors.RESET);

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
                    throw new Exception(ConsoleColors.RED_UNDERLINED + "Unable to create a directory." + ConsoleColors.RESET);
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
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "port is out of range." + ConsoleColors.RESET);
        if (cacheSize < 0)
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "cacheSize is out of range." + ConsoleColors.RESET);

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
                    throw new Exception(ConsoleColors.RED_UNDERLINED + "Unable to create a directory." + ConsoleColors.RESET);
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
            if (!colType.equals("int") && !colType.equals("text")) {
                throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "Invalid type for column " + colName + ": "  + ConsoleColors.RESET+ colType);
            }
            newTable.addCol(colName, colType);
        }
        return newTable;
    }

    public void removeKeys(){
        File dir = new File(dirPath);
        File[] db = dir.listFiles();
        for (File kv : db) {
            if(isCoordinatorOrReplicator(kv.getName())) continue;
            System.out.println(ConsoleColors.YELLOW_BOLD_UNDERLINED + "Deleting " + kv.getName());
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
                System.out.println(ConsoleColors.YELLOW_BOLD_UNDERLINED + "Moving " + key);
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

        System.out.println(ConsoleColors.GREEN_UNDERLINED + "setting hash ring" + ConsoleColors.RESET);
        this.hashRing = newHashRing;
        this.replicator.connect(newHashRing);
        this.removeKeys();

        try {
            this.moveKeys();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(ConsoleColors.RED_UNDERLINED + "Error occured when moving keys" + ConsoleColors.RESET);
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
            System.out.println(ConsoleColors.RED_UNDERLINED + "Error occured when replicating" + ConsoleColors.RESET);
        }
        return false;
    }

    public boolean replicateSQLCommand(String key, String value, StatusType status) throws Exception {
        try {
            return this.replicator.replicateSQLCommand(key, value, status);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(ConsoleColors.RED_UNDERLINED + "Error occured when replicating" + ConsoleColors.RESET);
        }
        return false;
    }

    public boolean replicateSQLTable(String key, String value) {
        try {
            return this.replicator.replicateSQLTable(key, value);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(ConsoleColors.RED_UNDERLINED + "Error occured when replicating" + ConsoleColors.RESET);
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
            throw new Exception(ConsoleColors.RED_UNDERLINED + "tuple not found" + ConsoleColors.RESET);

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
            throw new Exception(ConsoleColors.RED_UNDERLINED + "tuple not found" + ConsoleColors.RESET);

        return value;
    }

    @Override
    public synchronized StatusType putKV(String key, String value) throws Exception {
        if (write_lock) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception(ConsoleColors.RED_UNDERLINED + "unable to delete tuple" + ConsoleColors.RESET);

        File file = new File(dirPath + File.separator + escape(key));

        if (value.equals("null")) {
            File fileToDel = new File(dirPath, escape(key));
            if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
                throw new Exception(ConsoleColors.RED_UNDERLINED + "unable to delete tuple" + ConsoleColors.RESET);

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
            throw new Exception(ConsoleColors.RED_UNDERLINED + "unable to delete tuple" + ConsoleColors.RESET);

        File file = new File(dirPath + File.separator + escape(key));

        if (value.equals("null")) {
            File fileToDel = new File(dirPath, escape(key));
            if (!fileToDel.exists() || fileToDel.isDirectory() || !fileToDel.delete())
                throw new Exception(ConsoleColors.RED_UNDERLINED + "unable to delete tuple" + ConsoleColors.RESET);

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

    public synchronized StatusType sqlCreate(String key, String value, boolean override) throws Exception {
        if (write_lock & !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception(ConsoleColors.RED_UNDERLINED + "empty sql create value" + ConsoleColors.RESET);

        if (sqlTables.containsKey(key)) {
            throw new Exception(ConsoleColors.RED_UNDERLINED + "A table with the same name already exists" + ConsoleColors.RESET);
        }

        boolean validSqlCreate = true;
        String[] colPairs = value.split(",");
        Map<String, String> cols = new HashMap<>();
        String primaryKey = null;
        for (String pair : colPairs) {
            String[] parts = pair.split(":");
            if (parts.length != 2) {
                this.logger.error(ConsoleColors.RED_UNDERLINED + "Invalid column pair: " + pair + ConsoleColors.RESET);
                throw new Exception(ConsoleColors.RED_UNDERLINED + "Invalid column pair: " + pair + ConsoleColors.RESET);
            }
            String name = parts[0];
            String type = parts[1];
            if (!type.equals("int") && !type.equals("text")) {
                this.logger.error(ConsoleColors.RED_UNDERLINED + "Invalid type for column " + name + ": "  + ConsoleColors.RESET+ type);
                throw new Exception(ConsoleColors.RED_UNDERLINED + "Invalid type for column " + name + ": "  + ConsoleColors.RESET+ type);
            }
            if (cols.containsKey(name)) {
                this.logger.error(ConsoleColors.RED_UNDERLINED + "Column name " + name + " is repeated" + ConsoleColors.RESET);
                throw new Exception(ConsoleColors.RED_UNDERLINED + "Column name " + name + " is repeated" + ConsoleColors.RESET);
            }
            cols.put(name, type);
            if (primaryKey == null) {
                primaryKey = name;
            }
        }

        for (Map.Entry<String, String> entry : cols.entrySet()) {
            String name = entry.getKey();
            String type = entry.getValue();
            logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Column name: " + name + ", Type: "  + ConsoleColors.RESET+ type);
        }

        if (primaryKey == null) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "No primary key found" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "No primary key found" + ConsoleColors.RESET);
        }
        
        SQLTable table = createSQLTable(key, primaryKey, cols);
        sqlTables.put(key, table);

        // Print out the table
        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ table.toString() + ConsoleColors.RESET);

        return StatusType.SQLCREATE_SUCCESS;
    }

    public String sqlSelect(String key, boolean testing) throws Exception {
        SQLTable table = null;
        if (key.contains(" ")) {
            this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Parsing sql select query: " + key + ConsoleColors.RESET);
            String[] parts = key.split("\\s+from\\s+|\\s+where\\s+");
            if (parts.length < 2 || parts.length > 3) {
                throw new Exception(ConsoleColors.RED_UNDERLINED + "invalid sql select query" + ConsoleColors.RESET);
            }

            String columnNames = parts[0];
            String tableName = parts[1];
            String conds = parts.length == 3 ? parts[2] : null;
            this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ conds + ConsoleColors.RESET);

            List<SQLTable.Condition> conditions = new ArrayList<>();
            if (parts.length == 3) {
                this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Processing parts: " + Arrays.toString(parts) + ConsoleColors.RESET);
                String[] conditionParts = parts[2].replaceAll("[{}]", "").split(",");
                for (String conditionPart : conditionParts) {
                    this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Processing condition part: " + conditionPart + ConsoleColors.RESET);
                    Pattern pattern = Pattern.compile("(.*?)(=|<|>)(.*)");
                    Matcher matcher = pattern.matcher(conditionPart);
                    if (matcher.find()) {
                        String col = matcher.group(1).trim();
                        String operator = matcher.group(2).trim();
                        String value = matcher.group(3).trim();

                        SQLTable.Comparison comparison = SQLTable.getComparisonOperator(operator);

                        SQLTable.Condition condition = new SQLTable.Condition(col, value, comparison);    
                        conditions.add(condition);
                    } else {
                        this.logger.error(ConsoleColors.RED_UNDERLINED + "Failed to parse condition part: " + conditionPart + ConsoleColors.RESET);
                    }
                }
            } else {
                this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Parts length is not 3: " + Arrays.toString(parts) + ConsoleColors.RESET);
            }

            this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Conditions: " + conditions.size() + ConsoleColors.RESET);
            for (SQLTable.Condition condition : conditions) {
                this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Condition: " + condition.toString() + ConsoleColors.RESET);
            }

            table = sqlSelect(tableName, columnNames.equals("*") ? null : List.of(columnNames.replace("{", "").replace("}", "").split(",")), conditions);

        } else {
            if (!sqlTables.containsKey(key)) {
                throw new Exception(ConsoleColors.RED_UNDERLINED + "table not found" + ConsoleColors.RESET);
            }

            table = sqlTables.get(key);
        }

        if (testing) {
            return table.toStringForTransfer();
        } else {
            return table.toStringTable();
        }
    }

    public SQLTable sqlSelect(String tableName, List<String> columnNames, List<SQLTable.Condition> conditions) {
        SQLTable table = sqlTables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException(ConsoleColors.RED_UNDERLINED + "Table " + tableName + " does not exist." + ConsoleColors.RESET);
        }

        SQLTable selectedTable = table.selectRows(conditions);
        if (columnNames != null && !columnNames.isEmpty()) {
            selectedTable = selectedTable.selectCols(columnNames);
        }

        return selectedTable;
    }

    public synchronized StatusType sqlDrop(String key, boolean override) throws Exception {
        if (write_lock & !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (!sqlTables.containsKey(key)) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
        }

        try {
            sqlTables.remove(key);
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error dropping table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error dropping table: " + e.getMessage() + ConsoleColors.RESET);
        }

        return StatusType.SQLDROP_SUCCESS;
    }
    
    public synchronized StatusType sqlInsert(String key, String value, boolean override) throws Exception {
        if (write_lock && !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception(ConsoleColors.RED_UNDERLINED + "empty sql insert value" + ConsoleColors.RESET);

        if (!sqlTables.containsKey(key)) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
        }

        if (!checkValidJson(value)) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "table row has invalid formatting" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "table row has invalid formatting" + ConsoleColors.RESET);
        }

        SQLTable table = sqlTables.get(key);
        Map<String, String> rowMap = new HashMap<>();

        JsonElement jsonElement = null;
        try {
            jsonElement = jsonParser.parse(value);
        } catch (JsonParseException e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Invalid JSON format: " + e.getMessage() + ConsoleColors.RESET);
        }

        try {
            if (jsonElement != null && jsonElement.isJsonObject()) {
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                for (String jsonKey : jsonObject.keySet()) {
                    try {
                        JsonElement elem = jsonObject.get(jsonKey);
                        if (table.cols.contains(jsonKey)) {
                            String colValue = elem.getAsString();
                            if (table.colTypes.get(jsonKey).equals("int")) {
                                try {
                                    Integer.parseInt(colValue);
                                } catch (NumberFormatException e) {
                                    this.logger.error(ConsoleColors.RED_UNDERLINED + "Value for column " + jsonKey + " must be an integer" + ConsoleColors.RESET);
                                    throw new Exception(ConsoleColors.RED_UNDERLINED + "Value for column " + jsonKey + " must be an integer" + ConsoleColors.RESET);
                                }
                            }
                            rowMap.put(jsonKey, colValue);
                        } else {
                            this.logger.error(ConsoleColors.RED_UNDERLINED + jsonKey + " is not a column in table" + key + ConsoleColors.RESET);
                            throw new Exception(ConsoleColors.RED_UNDERLINED + jsonKey + " is not a column in table" + key + ConsoleColors.RESET);
                        }
                    } catch (Exception e) {
                        this.logger.error(ConsoleColors.RED_UNDERLINED + e.getMessage() + ConsoleColors.RESET);
                    }
                }
            }
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error adding row to table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error adding row to table: " + e.getMessage() + ConsoleColors.RESET);
        }

        try {
            table.addRow(rowMap);
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error adding row to table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error adding row to table: " + e.getMessage() + ConsoleColors.RESET);
        }

        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ table.toString() + ConsoleColors.RESET);
        return StatusType.SQLINSERT_SUCCESS;
    }
    
    public synchronized StatusType sqlUpdate(String key, String value, boolean override) throws Exception {
        if (write_lock && !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception(ConsoleColors.RED_UNDERLINED + "empty sql insert value" + ConsoleColors.RESET);


        if (!sqlTables.containsKey(key)) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "table does not exist" + ConsoleColors.RESET);
        }

        if (!checkValidJson(value)) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "table row has invalid formatting" + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "table row has invalid formatting" + ConsoleColors.RESET);
        }

        SQLTable table = sqlTables.get(key);
        Map<String, String> rowMap = new HashMap<>();

        JsonElement jsonElement = null;
        try {
            jsonElement = jsonParser.parse(value);
        } catch (JsonParseException e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Invalid JSON format: " + e.getMessage() + ConsoleColors.RESET);
        }

        try {
            if (jsonElement != null && jsonElement.isJsonObject()) {
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                for (String jsonKey : jsonObject.keySet()) {
                    try {
                        JsonElement elem = jsonObject.get(jsonKey);
                        if (table.cols.contains(jsonKey)) {
                            String colValue = elem.getAsString();
                            if (table.colTypes.get(jsonKey).equals("int")) {
                                try {
                                    Integer.parseInt(colValue);
                                } catch (NumberFormatException e) {
                                    this.logger.error(ConsoleColors.RED_UNDERLINED + "Value for column " + jsonKey + " must be an integer" + ConsoleColors.RESET);
                                    throw new Exception(ConsoleColors.RED_UNDERLINED + "Value for column " + jsonKey + " must be an integer" + ConsoleColors.RESET);
                                }
                            }
                            rowMap.put(jsonKey, colValue);
                        } else {
                            this.logger.error(ConsoleColors.RED_UNDERLINED + jsonKey + " is not a column in table" + key + ConsoleColors.RESET);
                            throw new Exception(ConsoleColors.RED_UNDERLINED + jsonKey + " is not a column in table" + key + ConsoleColors.RESET);
                        }
                    } catch (Exception e) {
                        this.logger.error(ConsoleColors.RED_UNDERLINED + e.getMessage() + ConsoleColors.RESET);
                    }
                }
            }
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error updating row in table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error updating row in table: " + e.getMessage() + ConsoleColors.RESET);
        }

        try {
            table.updateRow(rowMap);
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error updating row in table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error updating row in table: " + e.getMessage() + ConsoleColors.RESET);
        }

        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ table.toString() + ConsoleColors.RESET);
        return StatusType.SQLUPDATE_SUCCESS;
    }

    public synchronized StatusType sqlReplace(String key, String value, boolean override) throws Exception {
        if (write_lock && !override) {
            return StatusType.SERVER_WRITE_LOCK;
        }

        if (value.equals(""))
            throw new Exception(ConsoleColors.RED_UNDERLINED + "empty sql insert value" + ConsoleColors.RESET);

        if (sqlTables.containsKey(key)) {
            sqlTables.remove(key);
        }

        try {
            SQLTable table = SQLTable.fromString(value);
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error building table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error building table: " + e.getMessage() + ConsoleColors.RESET);
        }

        try {
            sqlTables.put(key, SQLTable.fromString(value));
        } catch (Exception e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Error putting table: " + e.getMessage() + ConsoleColors.RESET);
            throw new Exception(ConsoleColors.RED_UNDERLINED + "Error putting table: " + e.getMessage() + ConsoleColors.RESET);
        }

        return StatusType.SQLREPLICATE_SUCCESS;
    }

    /*
     * Helper Function to Monitor the State of Current KVs in Storage and Cache
     */
    public void printStorageAndCache() {
        File dir = new File(dirPath);
        File[] db = dir.listFiles();

        System.out.println(ConsoleColors.YELLOW_UNDERLINED + "Storage: ");
        if (db == null || db.length == 0)
            System.out.println(ConsoleColors.RED_UNDERLINED + "\tStorage is Empty." + ConsoleColors.RESET);
        else {
            for (File kv : db) {
                System.out.print("\t" + "Key: " + kv.getName() + ", "); // key
                try {
                    String content = new String(Files.readAllBytes(kv.toPath()));
                    System.out.println(ConsoleColors.YELLOW_UNDERLINED + "Value: " + content); // value
                } catch (IOException e) {
                    System.out.println(ConsoleColors.RED_UNDERLINED + "<Error>"); // could not access value for whatever reas + ConsoleColors.RESETon
                }
            }
        }

        System.out.println(ConsoleColors.YELLOW_UNDERLINED + "Cache: ");
        if (cache == null || cache.size() == 0)
            System.out.println(ConsoleColors.RED_UNDERLINED + "\tCache is Empty." + ConsoleColors.RESET);
        for (Map.Entry<String, String> kv : cache.entrySet())
            System.out.println(ConsoleColors.YELLOW_UNDERLINED + "\t" + "Key: " + kv.getKey() + ", Value: " + kv.getValue());

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
        HashMap<String, SQLTable> tables = new HashMap<>();
        while (ecsSocket != null && !ecsSocket.isClosed() && running) {
            ECSMessage message = messageService.receiveECSMessage(ecsSocket, this.ecsInStream);
            if (message == null) {
                // Connection has been closed by ECS, handle gracefully
                System.out.println(ConsoleColors.RED_UNDERLINED + "Socket connection closed, stopping listener." + ConsoleColors.RESET);
                break;
            }

            switch (message.getType()){
                case HASHRING: {
                    System.out.println(ConsoleColors.GREEN_UNDERLINED + "RECEIVED HASHRING COMMAND" + ConsoleColors.RESET);
                    this.setHashRing((ECSHashRing) message.getParameter("HASHRING"));
                    System.out.println(ConsoleColors.GREEN_BOLD_UNDERLINED + hashRing.toString() + ConsoleColors.RESET);

                    if(metadata != null) this.logger.info(ConsoleColors.RED_BOLD_UNDERLINED + "Old hashrange: " + metadata.toString() + ConsoleColors.RESET);
                    setMetadata(hashRing.getNodeForIdentifier(getHostaddress() + ":" + String.valueOf(this.getPort())));
                    if(metadata != null) this.logger.info(ConsoleColors.GREEN_BOLD_UNDERLINED + "Up to date hashrange: " + metadata.toString() + ConsoleColors.RESET);
                    break;
                }

                case TRANSFER_FROM:{
                    this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Received TRANSFER_FROM command from ECS" + ConsoleColors.RESET);
                    ECSNode toNode = (ECSNode) message.getParameter("TO_NODE");

                // get keys & tables to transfer
                    try{
                        kvPairs = getKVPairsNotResponsibleFor();
                        tables = getSQLTablesNotResponsibleFor();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    // transfer keys & tables
                    if ((kvPairs != null && kvPairs.size() > 0) || (tables != null && tables.size() > 0)){
                        messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.TRANSFER_TO, "TO_NODE", toNode, "KV_PAIRS", kvPairs, "SQL_TABLES", tables);
                    }
                    break;
                }
                
                case RECEIVE:{
                    this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Received RECIEVE command from ECS" + ConsoleColors.RESET);
                    ECSNode fromNode = (ECSNode) message.getParameter("FROM_NODE");
                    
                    this.write_lock = true;
                    
                    kvPairs = (HashMap<String, String>) message.getParameter("KV_PAIRS");
                    System.out.println(kvPairs);
                    if (kvPairs != null) {
                        for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                            try {
                                StatusType putStatus = putKV(entry.getKey(), entry.getValue(), true);
                                if (putStatus != StatusType.SERVER_WRITE_LOCK){
                                    if (this.replicate(entry.getKey(), entry.getValue())){
                                        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Replication success" + ConsoleColors.RESET);
                                    } else {
                                        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Replication failure" + ConsoleColors.RESET);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    tables = (HashMap<String, SQLTable>) message.getParameter("SQL_TABLES");
                    System.out.println(ConsoleColors.RED_UNDERLINED + "tables" + ConsoleColors.RESET);
                    if (tables != null) {
                        for (Map.Entry<String, SQLTable> entry : tables.entrySet()) {
                            SQLTable table = entry.getValue();
                            System.out.println(table.toStringTable());
                        }

                        for (Map.Entry<String, SQLTable> entry : tables.entrySet()) {
                            try {
                                StatusType sqlReplaceStatus = sqlReplace(entry.getKey(), entry.getValue().toStringForTransfer(), true);
                                if (sqlReplaceStatus != StatusType.SERVER_WRITE_LOCK){
                                    if (this.replicateSQLTable(entry.getKey(), entry.getValue().toStringForTransfer())){
                                        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "SQLREPLICATE_SUCCESS Replication success" + ConsoleColors.RESET);
                                    } else {
                                        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "SQLREPLICATE_FAILURE Replication failure" + ConsoleColors.RESET);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    
                    this.write_lock = false;
                    if(fromNode != null)
                        messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.TRANSFER_COMPLETE, "PING_NODE", fromNode);
                        break;
                }

                case TRANSFER_COMPLETE:{
                    this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Received TRANSFER_COMPLETE command from ECS" + ConsoleColors.RESET);
                    if (kvPairs != null) {
                        for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                            try {
                                putKV(entry.getKey(), "null");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    if (tables != null) {
                        for (Map.Entry<String, SQLTable> entry : tables.entrySet()) {
                            try {
                                sqlTables.remove(entry.getKey());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        break;
                    }
                }

                case SHUTDOWN_SERVER: {
                    System.out.println(ConsoleColors.RED_UNDERLINED + "Received SHUTDOWN_SERVER command" + ConsoleColors.RESET);
                    this.close();
                    break;
                }

                default:
                    System.out.println(ConsoleColors.RED_UNDERLINED + "Unrecognized Command in KVSERVER " + ConsoleColors.RESET);
            }
        }
    }

    public void connectECS() {
        if (ecsHost != null && ecsPort > -1) {
            try {
                ecsSocket = new Socket(ecsHost, ecsPort);
                this.logger.info(ConsoleColors.GREEN_UNDERLINED + "Connected to ECS at " + ecsHost + ":" + ecsPort + " via "
                        + ecsSocket.getInetAddress().getHostAddress()
                        + ":" + ecsSocket.getLocalPort() + ConsoleColors.RESET);


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
                            System.out.println(ConsoleColors.RED_UNDERLINED + "[KVServer] ECS Disconnected." + ConsoleColors.RESET);
                        }
                    }
                }).start();

            } catch (Exception e) {
                System.err.println(ConsoleColors.RED_UNDERLINED + "Error connecting to ECS" + ConsoleColors.RESET + ConsoleColors.RESET);
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
                this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "Started server listening at: " + "(" + serverSocket.getInetAddress().getHostName() + ")"
                        + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort()
                        + "; cache size: "
                        + cacheSize + "; cache strategy: " + strategy + "; ECS set to: " + ecsHost + ":" + ecsPort + ConsoleColors.RESET);

        } catch (IOException e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Server Socket cannot be opened: " + ConsoleColors.RESET);
            if (e instanceof BindException)
                this.logger.error(ConsoleColors.RED_UNDERLINED + "Port " + port + " is already bound." + ConsoleColors.RESET);
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
                    this.logger.info(ConsoleColors.GREEN_UNDERLINED+ "Connected to " + "(" + clientSocket.getInetAddress().getHostName() + ") "
                            + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + ConsoleColors.RESET);
                } catch (IOException e) {
                    this.logger.error(ConsoleColors.RED_UNDERLINED + "Unable to establish connection.\n" + ConsoleColors.RESET, e);
                }
            }
        }
        this.logger.info(ConsoleColors.RED_UNDERLINED + "Server is stopped." + ConsoleColors.RESET);
        close();
    }

    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            this.logger.error(ConsoleColors.RED_UNDERLINED + "Unable to close socket on port: " + port + ConsoleColors.RESET, e);
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
        HashMap<String, SQLTable> tables = null;
        System.out.println(ConsoleColors.RED_UNDERLINED + "Running shutdown hook" + ConsoleColors.RESET);
        try {
            kvPairs = getAllKVPairs();
            tables = sqlTables;
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if(this.hashRing != null && this.hashRing.getHashring().size() > 1 && this.ecsSocket != null){
                messageService.sendECSMessage(ecsSocket, this.ecsOutStream, ECSMessageType.SHUTDOWN, "KV_PAIRS", kvPairs, "SQL_TABLES", tables);
                for (Map.Entry<String, String> entry : kvPairs.entrySet()) {
                    putKV(entry.getKey(), "null");
                }
                File index = new File(dirPath);
                index.delete();

                sqlTables.clear();
            } else {
                System.out.println(ConsoleColors.RED_UNDERLINED + "Last server, not transferring KV Pairs and SQL Tables" + ConsoleColors.RESET);
            }
        } catch (Exception e) {
            System.out.println(ConsoleColors.RED_UNDERLINED + "GOT AN ERROR" + ConsoleColors.RESET + ConsoleColors.RESET);
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
        this.logger.info(ConsoleColors.YELLOW + "KVPairs not responsible for: " + kvPairs.toString() + ConsoleColors.RESET);
        return kvPairs;
    }

    public HashMap<String, SQLTable> getSQLTablesNotResponsibleFor() throws Exception {
        HashMap<String, SQLTable> sqlTablesNotResponsibleFor = new HashMap<>();
        for (Map.Entry<String, SQLTable> entry : sqlTables.entrySet()) {
            String key = entry.getKey();
            if (!metadata.isKeyInRange(key)) {
                sqlTablesNotResponsibleFor.put(key, entry.getValue());
            }
        }
        this.logger.info(ConsoleColors.YELLOW + "SQLTables not responsible for: " + sqlTablesNotResponsibleFor.toString() + ConsoleColors.RESET);
        return sqlTablesNotResponsibleFor;
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

    public HashMap<String, SQLTable> getAllSQLTablesResponsibleFor() throws Exception {
        HashMap<String, SQLTable> sqlTablesResponsibleFor = new HashMap<>();
        for (Map.Entry<String, SQLTable> entry : sqlTables.entrySet()) {
            String key = entry.getKey();
            if (metadata.isKeyInRange(key)) {
                sqlTablesResponsibleFor.put(key, entry.getValue());
            }
        }
        return sqlTablesResponsibleFor;
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
        this.logger.info(ConsoleColors.YELLOW_BOLD_UNDERLINED+ "KVPairs responsible for: " + kvPairs.toString() + ConsoleColors.RESET);
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