package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {
	private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "M1-Client> ";

	private static final int MAX_KEY_LEN = 20;
	private static final int MAX_KEY_VAL = 120 * 1024; // 120KB

    private BufferedReader stdin;
    private boolean stop = false;

    private KVStore kvStore = null; 

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub ///
        kvStore = new KVStore(hostname, port);
        kvStore.connect();
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return kvStore;
    }

    private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("M1 CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");

		sb.append(PROMPT).append("help");
		sb.append("\t\t\t\t display client cli commands and usage\n");

		sb.append(PROMPT).append("connect <address> <port>");
		sb.append("\t establish connection to server\n");

        sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t - insert a key-value pair into the server \n");
		sb.append("\t\t\t\t\t - update (overwrite) current value if server already contains key \n");
		sb.append("\t\t\t\t\t - delete entry for the given key if <value> = null \n");
        
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieve the value for the given key from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append("\t\t\t\t\t " + LogSetup.getPossibleLogLevels() + "\n");
		// sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnect from the server \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t stop the program");
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
		System.out.println(PROMPT + "Error! " +  error);
	}

	public void quit() {
    	logger.info("calling quit");
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

	private void handleCommand(String cmdLine) { 
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("help")) {
            printHelp();

		} else if (tokens[0].equals("quit")) {
            stop = true;
            quit();
            System.out.println(PROMPT + "Application stop!");

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
							System.out.println("PUT: " + tokens[1] + " " + value.toString());
                        } else {
                            printError("Invalid key-value pair!");
                            logger.error("Invalid key-value pair!");
                        }
                    } catch(Exception e) {
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
							System.out.println("GET: " + tokens[1]);
                        } else {
                            printError("Invalid key!");
                            logger.error("Invalid key!");
                        }
                    } catch(Exception e) {
                        logger.error("Get from server failed!", e);
                    }
                } else {
                    printError("Not connected!");
                }
            } else {
                if (tokens.length < 2) printError("No key passed!");
                if (tokens.length > 2) printError("Too many arguments!");
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
            kvStore.disconnect();

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
				logger.info(cmdLine);
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
