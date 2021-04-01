package org.hubspot.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

/**
 * @author Nicholas Curl
 */
public class CustomThreadFactory implements ThreadFactory {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private int counter;
    private final String name;

    public CustomThreadFactory(String name){
        counter = 1;
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name+"-thread_"+counter);
        counter++;
        return t;
    }
}
