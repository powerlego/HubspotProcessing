package org.hubspot.utils.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class CustomExecutors {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(CustomExecutors.class);

    public static ExecutorService newCachedThreadPool() {
        return new CustomThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }

    public static ExecutorService newCachedThreadPool(ThreadFactory factory) {
        return new CustomThreadPoolExecutor(0,
                                            Integer.MAX_VALUE,
                                            60L,
                                            TimeUnit.SECONDS,
                                            new SynchronousQueue<>(),
                                            factory
        );
    }

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new CustomThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    public static ExecutorService newFixedThreadPool(int nThreads, ThreadFactory factory) {
        return new CustomThreadPoolExecutor(nThreads,
                                            nThreads,
                                            0L,
                                            TimeUnit.MILLISECONDS,
                                            new LinkedBlockingQueue<>(),
                                            factory
        );
    }
}
