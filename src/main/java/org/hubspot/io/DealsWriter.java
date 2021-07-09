package org.hubspot.io;

import com.google.common.collect.Iterables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Deal;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.CacheThreadPoolExecutor;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * Writes the processed HubSpot deals to a file
 *
 * @Author: Anthony Prestia
 */
public class DealsWriter {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger(DealsWriter.class);
    /**
     * The folder to store the written engagements
     */
    private static final Path   dealsFolder  = Paths.get("./contacts/deals/");
    /**
     * The starting thread pool maximum size
     */
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    /**
     * The interval in milliseconds to update the maximum number of threads allowed in a thread pool
     */
    private static final long   UPDATE_INTERVAL    = 100;
    /**
     * The maximum limit for the maximum thread pool size
     */
    private static final int    MAX_SIZE           = 50;
    /**
     * The partition size limit
     */
    private static final int    LIMIT              = 1;
    /**
     * The format string used to help with debugging the load adjuster
     */
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    /**
     * Writes the deals to file and downloads any attachments if needed
     *
     * @param hubSpot   The instance of the Hubspot API
     * @param id        The contact id
     * @param deals     The list of deals to write
     */
    public static void write(HubSpot hubSpot, long id, List<Deal> deals) {
        logger.traceEntry("hubspot={}, id={}, deals={}", hubSpot, id, deals);
        /*Checks to see if the deals list is empty. If it is don't continue to write */
        if (!deals.isEmpty()) {
            /* Tries to create the overall directory that the deals are written to */
            try {
                Files.createDirectories(dealsFolder);
            }
            catch (IOException e) {
                logger.fatal(LogMarkers.ERROR.getMarker(),
                             "Unable to create deals folder {}",
                             dealsFolder,
                             e
                );
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            Path contact = dealsFolder.resolve(id + "/");
            /* Tries to create the directory based on the contact's id */
            try {
                Files.createDirectories(contact);
            }
            catch (IOException e) {
                logger.fatal(LogMarkers.ERROR.getMarker(),
                             "Unable to create contact directory for id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }

            /* Creates a thread pool to write the deals on a separate thread to increase execution speeds */
            int capacity = (int) Math.ceil(Math.ceil((double) deals.size() / (double) LIMIT) *
                                           Math.pow(MAX_SIZE, -0.6));
            CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                     STARTING_POOL_SIZE,
                                                                                     0L,
                                                                                     TimeUnit.MILLISECONDS,
                                                                                     new LinkedBlockingQueue<>(
                                                                                             Math.max(
                                                                                                     capacity,
                                                                                                     Runtime.getRuntime()
                                                                                                            .availableProcessors()
                                                                                             )),
                                                                                     new CustomThreadFactory(
                                                                                             "DealWriter"),
                                                                                     new StoringRejectedExecutionHandler(),
                                                                                     contact
            );
            Utils.addExecutor(threadPoolExecutor);
            Iterable<List<Deal>> partitions = Iterables.partition(deals, LIMIT);
            ScheduledExecutorService scheduledExecutorService =
                    Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementWriterUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "EngageWriter_write", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            /* Writes the deals to file on separate threads to increase execution speeds */
            for (List<Deal> partition: partitions) {
                threadPoolExecutor.submit(() -> {
                    for (Deal deal : partition) {
                        Path dealPath = dealsFolder.resolve(id +"/deal_" + deal.getId() + ".txt");
                        FileUtils.writeFile(dealPath, deal);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
        }
        logger.traceExit();
    }
}
