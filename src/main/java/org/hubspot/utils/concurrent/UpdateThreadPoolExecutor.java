package org.hubspot.utils.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.FileUtils;
import org.hubspot.utils.LogMarkers;
import org.hubspot.utils.exceptions.HubSpotException;

import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;

/**
 * @author Nicholas Curl
 */
public class UpdateThreadPoolExecutor extends CacheThreadPoolExecutor {

    /**
     * The instance of the logger
     */
    private static Logger logger = LogManager.getLogger();
    private        Path   folder;
    private        long   lastFinished;

    /**
     * Creates a new {@code ThreadPoolExecutor} with the given initial parameters, the default thread factory and the
     * default rejected execution handler.
     *
     * <p>It may be more convenient to use one of the {@link Executors}
     * factory methods instead of this general purpose constructor.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even if they are idle, unless {@code
     *                        allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime   when the number of threads is greater than the core, this is the maximum time that excess
     *                        idle threads will wait for new tasks before terminating.
     * @param unit            the time unit for the {@code keepAliveTime} argument
     * @param workQueue       the queue to use for holding tasks before they are executed.  This queue will hold only
     *                        the {@code Runnable} tasks submitted by the {@code execute} method.
     *
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     *                                  keepAliveTime < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize <
     *                                  corePoolSize}
     * @throws NullPointerException     if {@code workQueue} is null
     */
    public UpdateThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    Path folder,
                                    long lastFinished
    ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, folder);
        this.folder = folder;
        this.lastFinished = lastFinished;
    }

    /**
     * Creates a new {@code ThreadPoolExecutor} with the given initial parameters and {@linkplain AbortPolicy default
     * rejected execution handler}.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even if they are idle, unless {@code
     *                        allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime   when the number of threads is greater than the core, this is the maximum time that excess
     *                        idle threads will wait for new tasks before terminating.
     * @param unit            the time unit for the {@code keepAliveTime} argument
     * @param workQueue       the queue to use for holding tasks before they are executed.  This queue will hold only
     *                        the {@code Runnable} tasks submitted by the {@code execute} method.
     * @param threadFactory   the factory to use when the executor creates a new thread
     *
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     *                                  keepAliveTime < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize <
     *                                  corePoolSize}
     * @throws NullPointerException     if {@code workQueue} or {@code threadFactory} is null
     */
    public UpdateThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    ThreadFactory threadFactory,
                                    Path folder,
                                    long lastFinished
    ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, folder);
        this.folder = folder;
        this.lastFinished = lastFinished;
    }

    /**
     * Creates a new {@code ThreadPoolExecutor} with the given initial parameters and {@linkplain
     * Executors#defaultThreadFactory default thread factory}.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even if they are idle, unless {@code
     *                        allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime   when the number of threads is greater than the core, this is the maximum time that excess
     *                        idle threads will wait for new tasks before terminating.
     * @param unit            the time unit for the {@code keepAliveTime} argument
     * @param workQueue       the queue to use for holding tasks before they are executed.  This queue will hold only
     *                        the {@code Runnable} tasks submitted by the {@code execute} method.
     * @param handler         the handler to use when execution is blocked because the thread bounds and queue
     *                        capacities are reached
     *
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     *                                  keepAliveTime < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize <
     *                                  corePoolSize}
     * @throws NullPointerException     if {@code workQueue} or {@code handler} is null
     */
    public UpdateThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    RejectedExecutionHandler handler,
                                    Path folder,
                                    long lastFinished
    ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler, folder);
        this.folder = folder;
        this.lastFinished = lastFinished;
    }

    /**
     * Creates a new {@code ThreadPoolExecutor} with the given initial parameters.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even if they are idle, unless {@code
     *                        allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime   when the number of threads is greater than the core, this is the maximum time that excess
     *                        idle threads will wait for new tasks before terminating.
     * @param unit            the time unit for the {@code keepAliveTime} argument
     * @param workQueue       the queue to use for holding tasks before they are executed.  This queue will hold only
     *                        the {@code Runnable} tasks submitted by the {@code execute} method.
     * @param threadFactory   the factory to use when the executor creates a new thread
     * @param handler         the handler to use when execution is blocked because the thread bounds and queue
     *                        capacities are reached
     *
     * @throws IllegalArgumentException if one of the following holds:<br> {@code corePoolSize < 0}<br> {@code
     *                                  keepAliveTime < 0}<br> {@code maximumPoolSize <= 0}<br> {@code maximumPoolSize <
     *                                  corePoolSize}
     * @throws NullPointerException     if {@code workQueue} or {@code threadFactory} or {@code handler} is null
     */
    public UpdateThreadPoolExecutor(int corePoolSize,
                                    int maximumPoolSize,
                                    long keepAliveTime,
                                    TimeUnit unit,
                                    BlockingQueue<Runnable> workQueue,
                                    ThreadFactory threadFactory,
                                    RejectedExecutionHandler handler,
                                    Path folder,
                                    long lastFinished
    ) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, folder);
        this.folder = folder;
        this.lastFinished = lastFinished;
    }

    @Override
    protected void terminated() {
        Exception exception = super.getExecutionException();
        if (exception != null) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Error occurred during execution", exception);
            FileUtils.deleteRecentlyUpdated(folder, lastFinished);
            if (exception instanceof HubSpotException) {
                HubSpotException hubSpotException = (HubSpotException) exception;
                System.exit(hubSpotException.getCode());
            }
            else {
                System.exit(ErrorCodes.GENERAL.getErrorCode());
            }
        }
        else if (super.isInterrupted()) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Threads have been interrupted");
            FileUtils.deleteRecentlyUpdated(folder, lastFinished);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
        super.terminated();
    }
}
