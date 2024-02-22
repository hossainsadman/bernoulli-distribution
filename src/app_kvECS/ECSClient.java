package app_kvECS;

import java.io.*;
import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;

import java.net.ServerSocket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import org.apache.commons.cli.*;

public class ECSClient implements IECSClient {

	private static final int DEFAULT_ECS_PORT = 9999;

    private String address;
    private int port;

    private ServerSocket ecsSocket;

    private static Logger logger = Logger.getRootLogger();

    public ECSClient(String address, int port) {
        if (port < 1024 || port > 65535)
            throw new IllegalArgumentException("port is out of range.");
        
        this.address = address;
        this.port = port;
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();

        Option help = new Option("h", "help", false, "display help");
        help.setRequired(false);
        options.addOption(help);

        Option address = new Option("a", "address", true, "address to start ecs service");
        address.setRequired(false);
        options.addOption(address);

        Option port = new Option("p", "port", true, "server port");
        port.setRequired(false);
        options.addOption(port);
        
        Option logFile = new Option("l", "logFile", true, "log file path");
        logFile.setRequired(false);
        options.addOption(logFile);

        Option logLevel = new Option("ll", "logLevel", true, "log level");
        logLevel.setRequired(false);
        options.addOption(logLevel);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;    

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("m2-ecs.jar", options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("m2-ecs.jar", options);
            System.exit(0);
        }
        
        // set defaults for options
        String ecsAddress = cmd.getOptionValue("address", "localhost");
        String ecsPort = cmd.getOptionValue("port", String.valueOf(DEFAULT_ECS_PORT));
        String ecsLogFile = cmd.getOptionValue("logFile", "logs/ecs.log");
        String ecsLogLevel = cmd.getOptionValue("logLevel", "ALL");

        if (!LogSetup.isValidLevel(ecsLogLevel)) {
            ecsLogLevel = "ALL";
        }
        
        try {
            new LogSetup(ecsLogFile, LogSetup.getLogLevel(ecsLogLevel));
            ECSClient ecsclient = new ECSClient(ecsAddress, Integer. parseInt(ecsPort));
            logger.info("ECSClient started at " + address + ":" + port);
        } catch (Exception e) {
            e.printStackTrace();
        }        
    }
}
