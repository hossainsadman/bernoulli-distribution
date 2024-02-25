package ecs;

import org.apache.zookeeper.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

// Might need for later
public class ZKWatcher implements Watcher {
    private Logger logger;

    public ZKWatcher(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                logger.info("Connection event: " + event.getState());
                break;
            case NodeCreated:
                logger.info("Node created: " + event.getPath());
                break;
            case NodeDeleted:
                logger.info("Node deleted: " + event.getPath());
                break;
            case NodeDataChanged:
                logger.info("Node data changed: " + event.getPath());
                break;
            case NodeChildrenChanged:
                logger.info("Node children changed: " + event.getPath());
                break;
        }
    }
}
