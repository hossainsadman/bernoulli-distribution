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

    private BufferedReader stdin;
    private boolean exit = false;

    private KVStore kvstore = null; 

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub ///
        kvstore = new KVStore(hostname, port);
        kvstore.connect();
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return kvstore;
    }

    public void run() {
        while (!exit) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                System.out.println("Entered cmd: " + cmdLine);
            } catch (IOException e) {
                exit = true;
                System.out.println("I/O Error: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args){
        KVClient client = new KVClient();
        client.run();
    }
}
