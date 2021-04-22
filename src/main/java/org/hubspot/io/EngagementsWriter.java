package org.hubspot.io;

import com.google.common.collect.Iterables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.*;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.FileUtils;
import org.hubspot.utils.Utils;
import org.hubspot.utils.concurrent.CacheThreadPoolExecutor;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;
import org.hubspot.utils.exceptions.HubSpotException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Writes the processed Hubspot Engagements to a file
 *
 * @author Nicholas Curl
 */
public class EngagementsWriter {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    /**
     * The folder to store the written engagements
     */
    private static final Path   engagementsFolder  = Paths.get("./contacts/engagements/");
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
     * Writes the engagements to file and downloads any attachments if needed
     *
     * @param hubspot     The instance of the Hubspot API
     * @param id          The contact id
     * @param engagements The list of engagements to write
     */
    public static void write(final HubSpot hubspot,
                             final long id,
                             final List<Engagement> engagements
    ) {
        /*Checks to see if the engagements list is empty. If it is don't continue to write */
        if (!engagements.isEmpty()) {
            /* Tries to create the overall directory that the engagements are written to*/
            try {
                Files.createDirectories(engagementsFolder);
            }
            catch (final IOException e) {
                logger.fatal("Unable to create engagements folder {}", engagementsFolder, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            final Path contact = engagementsFolder.resolve(id + "/");
            /* Tries to create the directory based on the contact's id*/
            try {
                Files.createDirectories(contact);
            }
            catch (final IOException e) {
                logger.fatal("Unable to create contact directory for id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            final Path emails = contact.resolve("emails/");
            final Path notes = contact.resolve("notes/");
            final Path tasks = contact.resolve("tasks/");
            final Path calls = contact.resolve("calls/");
            final Path meetings = contact.resolve("meetings/");
            /* Tries to create the sub-directories to store specific engagements in*/
            try {
                Files.createDirectories(emails);
            }
            catch (final IOException e) {
                logger.fatal("Unable to make emails directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(notes);
            }
            catch (final IOException e) {
                logger.fatal("Unable to make notes directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(tasks);
            }
            catch (final IOException e) {
                logger.fatal("Unable to make tasks directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(calls);
            }
            catch (final IOException e) {
                logger.fatal("Unable to make calls directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(meetings);
            }
            catch (final IOException e) {
                logger.fatal("Unable to make meetings directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            final AtomicInteger emailNum = new AtomicInteger();
            final AtomicInteger noteNum = new AtomicInteger();
            final AtomicInteger meetingNum = new AtomicInteger();
            final AtomicInteger callNum = new AtomicInteger();
            final AtomicInteger taskNum = new AtomicInteger();
            final int capacity = (int) Math.ceil(Math.ceil((double) engagements.size() / (double) LIMIT) *
                                                 Math.pow(MAX_SIZE, -0.6));
            /*Creates a thread pool to write the engagements on a separate thread to increase execution speed*/
            final CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
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
                                                                                                   "EngagementWriter"),
                                                                                           new StoringRejectedExecutionHandler(),
                                                                                           contact
            );
            final Iterable<List<Engagement>> partitions = Iterables.partition(engagements, LIMIT);
            final ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementWriterUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                final double load = CPUMonitor.getProcessLoad();
                final String debugMessage = String.format(debugMessageFormat, "EngageWriter_write", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            /* Writes the engagements to file on a separate threads to increase execution speed*/
            for (final List<Engagement> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final Engagement engagement : partition) {
                        if (engagement instanceof Email) {
                            final Email email = (Email) engagement;
                            final Path emailPath = emails.resolve("email_" + emailNum.get() + ".txt");
                            FileUtils.writeFile(emailPath, email);
                            emailNum.getAndIncrement();
                        }
                        else if (engagement instanceof Note) {
                            final Note note = (Note) engagement;
                            final Path notePath;
                            /* Checks to see if the note has attachments.  If it does download them.*/
                            if (note.hasAttachments()) {
                                logger.trace("Contact ID {}, Note Num {}", id, noteNum.get());
                                final Path noteFolder = notes.resolve("note_" + noteNum.get());
                                try {
                                    Files.createDirectories(noteFolder);
                                }
                                catch (final IOException e) {
                                    throw new HubSpotException("Unable to create directory " + noteFolder,
                                                               ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode(),
                                                               e
                                    );
                                }
                                notePath = noteFolder.resolve("note_" + noteNum.get() + ".txt");
                                hubspot.cms().downloadFiles(noteFolder, note.getAttachments());
                            }
                            else {
                                notePath = notes.resolve("note_" + noteNum.get() + ".txt");
                            }
                            FileUtils.writeFile(notePath, note);
                            noteNum.getAndIncrement();
                        }
                        else if (engagement instanceof Meeting) {
                            final Meeting meeting = (Meeting) engagement;
                            final Path meetingPath = meetings.resolve("meeting_" + meetingNum.get() + ".txt");
                            FileUtils.writeFile(meetingPath, meeting);
                            meetingNum.getAndIncrement();
                        }
                        else if (engagement instanceof Call) {
                            final Call call = (Call) engagement;
                            final Path callPath = calls.resolve("call_" + callNum.get() + ".txt");
                            FileUtils.writeFile(callPath, call);
                            callNum.getAndIncrement();
                        }
                        else if (engagement instanceof Task) {
                            final Task task = (Task) engagement;
                            final Path taskPath = tasks.resolve("task_" + taskNum.get() + ".txt");
                            FileUtils.writeFile(taskPath, task);
                            taskNum.getAndIncrement();
                        }
                        else {
                            throw new HubSpotException("Unknown engagement",
                                                       ErrorCodes.INVALID_ENGAGEMENT.getErrorCode()
                            );
                        }
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
            final File[] callFiles = calls.toFile().listFiles();
            final File[] emailFiles = emails.toFile().listFiles();
            final File[] noteFiles = notes.toFile().listFiles();
            final File[] meetingFiles = meetings.toFile().listFiles();
            final File[] taskFiles = tasks.toFile().listFiles();
            /* Deletes any blank directories so that it the engagements storage is easier to understand*/
            if (callFiles == null || callFiles.length == 0) {
                FileUtils.deleteDirectory(calls);
            }
            if (emailFiles == null || emailFiles.length == 0) {
                FileUtils.deleteDirectory(emails);
            }
            if (noteFiles == null || noteFiles.length == 0) {
                FileUtils.deleteDirectory(notes);
            }
            if (meetingFiles == null || meetingFiles.length == 0) {
                FileUtils.deleteDirectory(meetings);
            }
            if (taskFiles == null || taskFiles.length == 0) {
                FileUtils.deleteDirectory(tasks);
            }
        }
    }
}
