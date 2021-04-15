package org.hubspot.utils.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * @author Nicholas Curl
 */
public class AutoShutdown<V> implements Callable<V> {

    /**
     * The instance of the logger
     */
    private static final Logger          logger = LogManager.getLogger();
    private final        ExecutorService executorService;
    private final        Callable<V>     task;

    public AutoShutdown(final ExecutorService executorService, final Callable<V> task) {
        this.executorService = executorService;
        this.task = task;
    }

    @Override
    public V call() throws Exception {
        try {
            return task.call();
        }
        catch (Exception e) {
            executorService.shutdownNow();
            throw e;
        }
    }
}
