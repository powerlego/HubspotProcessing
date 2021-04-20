package org.hubspot.io;

import com.google.common.collect.Iterables;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.*;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.ErrorCodes;
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
 * @author Nicholas Curl
 */
public class EngagementsWriter {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    private static final Path   engagementsFolder  = Paths.get("./contacts/engagements/");
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    MAX_SIZE           = 50;
    private static final int    LIMIT              = 1;
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static void write(HubSpot hubspot, long id, List<Engagement> engagements) {
        if (!engagements.isEmpty()) {
            try {
                Files.createDirectories(engagementsFolder);
            }
            catch (IOException e) {
                logger.fatal("Unable to create engagements folder {}", engagementsFolder, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            Path contact = engagementsFolder.resolve(id + "/");
            try {
                Files.createDirectories(contact);
            }
            catch (IOException e) {
                logger.fatal("Unable to create contact directory for id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            Path emails = contact.resolve("emails/");
            Path notes = contact.resolve("notes/");
            Path tasks = contact.resolve("tasks/");
            Path calls = contact.resolve("calls/");
            Path meetings = contact.resolve("meetings/");
            try {
                Files.createDirectories(emails);
            }
            catch (IOException e) {
                logger.fatal("Unable to make emails directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(notes);
            }
            catch (IOException e) {
                logger.fatal("Unable to make notes directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(tasks);
            }
            catch (IOException e) {
                logger.fatal("Unable to make tasks directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(calls);
            }
            catch (IOException e) {
                logger.fatal("Unable to make calls directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            try {
                Files.createDirectories(meetings);
            }
            catch (IOException e) {
                logger.fatal("Unable to make meetings directory for contact id {}", id, e);
                System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
            }
            AtomicInteger emailNum = new AtomicInteger();
            AtomicInteger noteNum = new AtomicInteger();
            AtomicInteger meetingNum = new AtomicInteger();
            AtomicInteger callNum = new AtomicInteger();
            AtomicInteger taskNum = new AtomicInteger();
            int capacity = (int) Math.ceil(Math.ceil((double) engagements.size() / (double) LIMIT) *
                                           Math.pow(MAX_SIZE, -0.6));
            CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                     STARTING_POOL_SIZE,
                                                                                     0L,
                                                                                     TimeUnit.MILLISECONDS,
                                                                                     new LinkedBlockingQueue<>(Math.max(
                                                                                             capacity,
                                                                                             Runtime.getRuntime()
                                                                                                    .availableProcessors()
                                                                                     )),
                                                                                     new CustomThreadFactory(
                                                                                             "EngagementWriter"),
                                                                                     new StoringRejectedExecutionHandler(),
                                                                                     contact
            );
            Iterable<List<Engagement>> partitions = Iterables.partition(engagements, LIMIT);
            ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementWriterUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "EngageWriter_write", load);
                logger.trace(debugMessage);
                int comparison = Double.compare(load, 50.0);
                if (comparison > 0 && threadPoolExecutor.getMaximumPoolSize() != threadPoolExecutor.getCorePoolSize()) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
                else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != MAX_SIZE) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            for (List<Engagement> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (Engagement engagement : partition) {
                        if (engagement instanceof Email) {
                            Email email = (Email) engagement;
                            Path emailPath = emails.resolve("email_" + emailNum.get() + ".txt");
                            Utils.writeFile(emailPath, email);
                            emailNum.getAndIncrement();
                        }
                        else if (engagement instanceof Note) {
                            Note note = (Note) engagement;
                            Path notePath;
                            if (note.hasAttachments()) {
                                logger.trace("Contact ID {}, Note Num {}", id, noteNum.get());
                                Path noteFolder = notes.resolve("note_" + noteNum.get());
                                try {
                                    Files.createDirectories(noteFolder);
                                }
                                catch (IOException e) {
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
                            Utils.writeFile(notePath, note);
                            noteNum.getAndIncrement();
                        }
                        else if (engagement instanceof Meeting) {
                            Meeting meeting = (Meeting) engagement;
                            Path meetingPath = meetings.resolve("meeting_" + meetingNum.get() + ".txt");
                            Utils.writeFile(meetingPath, meeting);
                            meetingNum.getAndIncrement();
                        }
                        else if (engagement instanceof Call) {
                            Call call = (Call) engagement;
                            Path callPath = calls.resolve("call_" + callNum.get() + ".txt");
                            Utils.writeFile(callPath, call);
                            callNum.getAndIncrement();
                        }
                        else if (engagement instanceof Task) {
                            Task task = (Task) engagement;
                            Path taskPath = tasks.resolve("task_" + taskNum.get() + ".txt");
                            Utils.writeFile(taskPath, task);
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
            File[] callFiles = calls.toFile().listFiles();
            File[] emailFiles = emails.toFile().listFiles();
            File[] noteFiles = notes.toFile().listFiles();
            File[] meetingFiles = meetings.toFile().listFiles();
            File[] taskFiles = tasks.toFile().listFiles();
            if (callFiles == null || callFiles.length == 0) {
                Utils.deleteDirectory(calls);
            }
            if (emailFiles == null || emailFiles.length == 0) {
                Utils.deleteDirectory(emails);
            }
            if (noteFiles == null || noteFiles.length == 0) {
                Utils.deleteDirectory(notes);
            }
            if (meetingFiles == null || meetingFiles.length == 0) {
                Utils.deleteDirectory(meetings);
            }
            if (taskFiles == null || taskFiles.length == 0) {
                Utils.deleteDirectory(tasks);
            }
        }
    }
}
