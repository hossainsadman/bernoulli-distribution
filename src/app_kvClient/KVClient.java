package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import java.util.Map;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;
import shared.messages.BasicKVMessage;
import shared.messages.KVMessage;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "M4-Client> ";

    private static final int MAX_KEY_LEN = 20;
    private static final int MAX_KEY_VAL = 120 * 1024; // 120KB

    private BufferedReader stdin;
    private boolean stop = false;

    private KVStore kvStore = null;

    private static Gson gson = new Gson();
    private static JsonParser jsonParser = new JsonParser();

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    @Override
    public KVCommInterface getStore() {
        return kvStore;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("M2 CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");

        sb.append(PROMPT).append("help");
        sb.append("\t\t\t\t display client cli commands and usage\n");

        sb.append(PROMPT).append("connect <address> <port>");
        sb.append("\t establish connection to server\n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t - insert a key-value pair into the server \n");
        sb.append(PROMPT).append("\t\t\t\t - update (overwrite) current value if server already contains key \n");
        sb.append(PROMPT).append("\t\t\t\t - delete entry for the given key if <value> = null \n");

        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieve the value for the given key from the server \n");

        sb.append(PROMPT).append("keyrange");
        sb.append("\t\t\t retrieve keyranges for all servers \n");

        sb.append(PROMPT).append("keyrange_read");
        sb.append("\t\t\t retrieve key ranges of the KV Servers including replicas \n");

        sb.append(PROMPT).append("sql <query>");
        sb.append("\t\t\t run sql commands (create table, update, add/remove rows) \n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t " + LogSetup.getPossibleLogLevels() + "\n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnect from the server \n");

        sb.append(PROMPT).append("quit");
        sb.append("\t\t\t\t stop the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println("\t\t" + LogSetup.getPossibleLogLevels());
    }

    private String setLogLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    public void quit() {
        logger.info("Disconnecting from server and shutting down client...");
        if (kvStore != null) {
            kvStore.disconnect();
            kvStore = null;
        }
        stop = true;
        return;
    }

    public boolean checkValidKey(String key, String value) {
        if (key == null || key.isEmpty() || key.length() > MAX_KEY_LEN
                || value.length() > MAX_KEY_VAL) {
            return false;
        }
        return true;
    }

    public boolean checkValidKey(String key) {
        if (key == null || key.isEmpty() || key.length() > MAX_KEY_LEN) {
            return false;
        }
        return true;
    }

    public static boolean checkValidJson(String str) {
        try {
            jsonParser.parse(str);
            return true;
        } catch (JsonSyntaxException e) {
            return false;
        }
    }

    public boolean checkValidSqlSelect(String cmd) {        
        String regex = "(\\*|\\{[^}]*\\}) from [^ ]+( where \\{\\s*([a-zA-Z0-9 ]+\\s*[<>=]\\s*[a-zA-Z0-9 ]+\\s*(,\\s*[a-zA-Z0-9 ]+\\s*[<>=]\\s*[a-zA-Z0-9 ]+\\s*)*)?\\}\\s*)?";
        return cmd.matches(regex);
    }

    private void handleCommand(String cmdLine) {
        if (cmdLine.trim().isEmpty()) {
            return;
        }
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("clear")){
            System.out.print("\033[H\033[2J");  
            System.out.flush();  
        } else if (tokens[0].equals("help")) {
            printHelp();

        } else if (tokens[0].equals("quit")) {
            stop = true;
            quit();
            System.out.println(PROMPT + "Application stop!");

        } else if (tokens[0].equals("keyrange")) {
            if (kvStore != null) {
                try {
                    KVMessage msg = kvStore.keyrange();
                    if (msg != null) {
                        System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey());
                    } else {
                        System.out.println(PROMPT + "KEYRANGE ERROR: null msg!");
                    }
                } catch (Exception e) {
                    logger.error("Put to server failed!", e);
                }
            } else {
                printError("Not connected to server!");
            }
        } else if (tokens[0].equals("keyrange_read")){
            if (kvStore != null){
                try {
                    KVMessage msg = kvStore.keyrangeRead();
                    if (msg != null) {
                        System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey());
                    } else {
                        System.out.println(PROMPT + "KEYRANGE_READ ERROR: null msg!");
                    }
                } catch (Exception e) {
                    logger.error("Put to server failed!", e);
                }
            } else {
                printError("Not connected to server!");
            }
        } else if (tokens[0].equals("get_all_keys")){
            if (kvStore != null){
                try {
                    KVMessage msg = kvStore.getAllKeys();
                    if (msg != null) {
                        System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey());
                    } else {
                        System.out.println(PROMPT + "GET_ALL_KEYS ERROR: null msg!");
                    }
                } catch (Exception e) {
                    logger.error("Put to server failed!", e);
                }
            } else {
                printError("Not connected to server!");
            }
        } else if (tokens[0].equals("sqlcreate")) {
            if (tokens.length >= 3) {
                if (kvStore != null) {
                    String tableName = tokens[1];
                    StringBuilder colPairs = new StringBuilder();
                    colPairs.setLength(0);

                    for (int i = 2; i < tokens.length; i++) {
                        colPairs.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            colPairs.append(" ");
                        }
                    }

                    try {
                        boolean validSqlCreate = true;
                        String[] pairs = colPairs.toString().split(",");
                        Map<String, String> cols = new HashMap<>();
                        for (String pair : pairs) {
                            String[] parts = pair.split(":");
                            if (parts.length != 2) {
                                System.out.println(PROMPT + "Invalid column pair: " + pair);
                                validSqlCreate = false;
                            }
                            String name = parts[0];
                            String type = parts[1];
                            if (!type.equals("int") && !type.equals("text")) {
                                System.out.println(PROMPT + "Invalid type for column " + name + ": " + type);
                                validSqlCreate = false;
                            }
                            if (cols.containsKey(name)) {
                                System.out.println(PROMPT + "Column name " + name + " is repeated");
                                validSqlCreate = false;
                            }
                            cols.put(name, type);
                        }

                        if (validSqlCreate) {
                            KVMessage msg = kvStore.sqlcreate(tableName, colPairs.toString());
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqlcreate ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid sqlcreate!");
                            logger.error("Invalid sqlcreate!");
                        }
                    } catch (Exception e) {
                        logger.error("sqlcreate to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("No sqlcreate values provided!");
            }
        } else if (tokens[0].equals("sqlselect")) {
            if (tokens.length == 2) {
                if (kvStore != null) {
                    String tableName = tokens[1];
                    
                    try {
                        if (checkValidKey(tableName)) {
                            KVMessage msg = kvStore.sqlselect(tableName, false);
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqlselect ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid sqlselect!");
                            logger.error("Invalid sqlselect!");
                        }
                    } catch (Exception e) {
                        logger.error("sqlselect to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else if (tokens.length > 2) {
                // if (kvStore != null) {
                    StringBuilder row = new StringBuilder();
                    for (int i = 1; i < tokens.length; i++) {
                        row.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            row.append(" ");
                        }
                    }

                    if (checkValidSqlSelect(row.toString())) {
                        try {
                            KVMessage msg = kvStore.sqlselect(row.toString(), false);
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqlselect ERROR: null msg!");
                            }
                        } catch (Exception e) {
                            logger.error("sqlselect to server failed!", e);
                        }
                    } else {
                        printError("Invalid sqlselect!");
                        logger.error("Invalid sqlselect!");
                    }
                // } else {
                    // printError("Not connected to server!");
                // }   
            } else {
                printError("No sqlselect values provided!");
            }
        } else if (tokens[0].equals("sqldrop")) {
            if (tokens.length == 2) {
                if (kvStore != null) {
                    String tableName = tokens[1];
                    
                    try {
                        if (checkValidKey(tableName)) {
                            KVMessage msg = kvStore.sqldrop(tableName);
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqldrop ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid sqldrop!");
                            logger.error("Invalid sqldrop!");
                        }
                    } catch (Exception e) {
                        logger.error("sqldrop to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("No sqldrop values provided!");
            }
        } else if (tokens[0].equals("sqlinsert")) {
            if (tokens.length >= 3) {
                if (kvStore != null) {
                    String tableName = tokens[1];
                    StringBuilder row = new StringBuilder();
                    for (int i = 2; i < tokens.length; i++) {
                        row.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            row.append(" ");
                        }
                    }
                    
                    try {
                        boolean valid = true;
                        if (!checkValidKey(tableName)) {
                            printError("Invalid sqlinsert table name!");
                            logger.error("Invalid sqlinsert table name!");
                            valid = false;
                        }
                        if (!checkValidJson(row.toString())) {
                            printError("Invalid sqlinsert table row!");
                            logger.error("Invalid sqlinsert table row!");
                            valid = false;
                        }

                        if (valid) {
                            KVMessage msg = kvStore.sqlinsert(tableName, row.toString());
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqlinsert ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid sqlinsert!");
                            logger.error("Invalid sqlinsert!");
                        }

                    } catch (Exception e) {
                        logger.error("sqlinsert to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("No sqlinsert values provided!");
            }
        } else if (tokens[0].equals("sqlupdate")) {
            if (tokens.length >= 3) {
                if (kvStore != null) {
                    String tableName = tokens[1];
                    StringBuilder row = new StringBuilder();
                    for (int i = 2; i < tokens.length; i++) {
                        row.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            row.append(" ");
                        }
                    }
                    
                    try {
                        boolean valid = true;
                        if (!checkValidKey(tableName)) {
                            printError("Invalid sqlupdate table name!");
                            logger.error("Invalid sqlupdate table name!");
                            valid = false;
                        }
                        if (!checkValidJson(row.toString())) {
                            printError("Invalid sqlupdate table row!");
                            logger.error("Invalid sqlupdate table row!");
                            valid = false;
                        }

                        if (valid) {
                            KVMessage msg = kvStore.sqlupdate(tableName, row.toString());
                            if (msg != null) {
                                System.out.println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "sqlupdate ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid sqlupdate!");
                            logger.error("Invalid sqlupdate!");
                        }

                    } catch (Exception e) {
                        logger.error("sqlupdate to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("No sqlupdate values provided!");
            }
        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                String serverAddress = tokens[1];
                try {
                    int serverPort = Integer.parseInt(tokens[2]);
                    this.newConnection(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("Invalid address. Port must be a number!");
                } catch (IOException e) {
                    printError("Invalid input!");
                } catch (Exception e) {
                    printError("Could not establish connection!");
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (kvStore != null) {
                    StringBuilder value = new StringBuilder();
                    value.setLength(0);

                    for (int i = 2; i < tokens.length; i++) {
                        value.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            value.append(" ");
                        }
                    }

                    try {
                        if (checkValidKey(tokens[1], value.toString())) {
                            KVMessage msg = kvStore.put(tokens[1], value.toString());
                            if (msg != null) {
                                System.out
                                        .println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "PUT ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid key-value pair!");
                            logger.error("Invalid key-value pair!");
                        }
                    } catch (Exception e) {
                        logger.error("Put to server failed!", e);
                    }
                } else {
                    printError("Not connected to server!");
                }
            } else {
                printError("No key-value pair provided!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (kvStore != null) {
                    try {
                        if (checkValidKey(tokens[1])) {
                            BasicKVMessage msg = kvStore.get(tokens[1]);
                            if (msg != null) {
                                System.out
                                        .println(PROMPT + msg.getStatus() + " " + msg.getKey() + " " + msg.getValue());
                            } else {
                                System.out.println(PROMPT + "PUT ERROR: null msg!");
                            }
                        } else {
                            printError("Invalid key!");
                            logger.error("Invalid key!");
                        }
                    } catch (Exception e) {
                        logger.error("Get from server failed!", e);
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                if (tokens.length < 2)
                    printError("No key passed!");
                if (tokens.length > 2)
                    printError("Too many arguments!");
            }

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                if (LogSetup.isValidLevel(tokens[1])) {
                    String level = setLogLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT + "Set log level: " + level);
                    }
                } else {
                    printError("Invalid log level!");
                    printPossibleLogLevels();
                }
            } else {
                printError("Invalid number of arguments!");
            }

        } else if (tokens[0].equals("disconnect")) {
            try {
                if (kvStore != null) {
                    kvStore.disconnect();
                    kvStore = null;
                    System.out.println(PROMPT + "Disconnected from server!");
                } else {
                    printError("Not connected to server!");
                }
            } catch (Exception e) {
                logger.error("Disconnect from server failed!", e);
            }

        } else if (tokens[0].length() == 0) {
            // do nothing

        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                logger.error("I/O Error: " + e.getMessage());
                printError(e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient client = new KVClient();
            client.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
