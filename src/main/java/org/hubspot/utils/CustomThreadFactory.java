package org.hubspot.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;

/**
 * @author Nicholas Curl
 */
public class CustomThreadFactory implements ThreadFactory {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final String name;
    private int counter;

    public CustomThreadFactory(String name) {
        counter = 1;
        this.name = name;
    }

    @Override
    public Thread newThread(@NotNull Runnable r) {
        Thread t = new Thread(r, name + "-thread_" + counter);
        counter++;
        return t;
    }
}
