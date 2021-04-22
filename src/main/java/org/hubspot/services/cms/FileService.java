package org.hubspot.services.cms;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.Engagement;
import org.hubspot.objects.crm.engagements.Note;
import org.hubspot.objects.files.Document;
import org.hubspot.objects.files.HSFile;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.CacheThreadPoolExecutor;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.CustomThreadPoolExecutor;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;
import org.hubspot.utils.exceptions.HubSpotException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class FileService {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    private static final Path   cacheFolder        = Paths.get("./cache/files/");
    private static final int    LIMIT              = 1;
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";
    private static final int    MAX_SIZE           = 50;

    static void downloadFiles(final Path folder,
                              final List<HSFile> files,
                              final HttpService httpService,
                              final RateLimiter rateLimiter
    )
    throws HubSpotException {
        for (final HSFile hsFile : files) {
            downloadFile(folder, hsFile, httpService, rateLimiter);
        }
    }

    static void downloadFile(final Path folder,
                             final HSFile file,
                             final HttpService httpService,
                             final RateLimiter rateLimiter
    )
    throws HubSpotException {

        if (file != null) {
            final String requestUrl = "/filemanager/api/v3/files/" + file.getId() + "/signed-url";
            rateLimiter.acquire(1);
            final JSONObject resp = (JSONObject) httpService.getRequest(requestUrl);
            final String downloadUrlString = resp.getString("url");
            FileUtils.downloadFile(downloadUrlString, folder, file);
        }
    }

    static void getAllNoteAttachments(final HttpService httpService,
                                      final RateLimiter rateLimiter,
                                      final long contactId,
                                      final List<Engagement> engagements
    ) {
        final Path folder = cacheFolder.resolve(contactId + "/");
        try {
            Files.createDirectories(folder);
        }
        catch (final IOException e) {
            logger.fatal("Unable to create cache directory {}", folder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        final ArrayList<Note> notes = new ArrayList<>();
        for (final Engagement engagement : engagements) {
            if (engagement instanceof Note) {
                final Note note = (Note) engagement;
                notes.add(note);
            }
        }
        final int capacity = (int) Math.ceil(Math.ceil((double) notes.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final Iterable<List<Note>> partitions = Iterables.partition(notes, LIMIT);
        final CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                       STARTING_POOL_SIZE,
                                                                                       0L,
                                                                                       TimeUnit.MILLISECONDS,
                                                                                       new LinkedBlockingQueue<>(Math.max(
                                                                                               capacity,
                                                                                               Runtime.getRuntime()
                                                                                                      .availableProcessors()
                                                                                       )),
                                                                                       new CustomThreadFactory(
                                                                                               "FileGrabber_Contact_" +
                                                                                               contactId),
                                                                                       new StoringRejectedExecutionHandler(),
                                                                                       folder
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("FileGrabberUpdater_Contact_" +
                                                                                     contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getAllNoteAttachments", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        for (final List<Note> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final Note note : partition) {
                    final Path noteFolder = folder.resolve(note.getId() + "/");
                    final List<HSFile> attachments = getFileMetadatas(httpService, rateLimiter, noteFolder, note);
                    note.setAttachments(attachments);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
    }

    private static List<HSFile> getFileMetadatas(final HttpService httpService,
                                                 final RateLimiter rateLimiter,
                                                 final Path folder,
                                                 final Note note
    )
    throws HubSpotException {
        try {
            Files.createDirectories(folder);
        }
        catch (final IOException e) {
            throw new HubSpotException("Unable to create attachment cache directory",
                                       ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode(),
                                       e
            );
        }
        final List<Long> attachmentIds = note.getAttachmentIds();
        final int capacity = (int) Math.ceil(Math.ceil((double) attachmentIds.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final Iterable<List<Long>> partitions = Iterables.partition(attachmentIds, LIMIT);
        final CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                       STARTING_POOL_SIZE,
                                                                                       0L,
                                                                                       TimeUnit.MILLISECONDS,
                                                                                       new LinkedBlockingQueue<>(Math.max(
                                                                                               capacity,
                                                                                               Runtime.getRuntime()
                                                                                                      .availableProcessors()
                                                                                       )),
                                                                                       new CustomThreadFactory(
                                                                                               "FileMetadataGrabber"),
                                                                                       new StoringRejectedExecutionHandler(),
                                                                                       folder
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("FileMetadataGrabberUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getFileMetadatas", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        final List<HSFile> attachments = Collections.synchronizedList(new ArrayList<>());
        for (final List<Long> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final Long fileId : partition) {
                    final Path cacheFile = folder.resolve(fileId + ".json");
                    final String url = "/filemanager/api/v2/files/" + fileId;
                    rateLimiter.acquire(1);
                    final JSONObject metadata = (JSONObject) httpService.getRequest(url);
                    org.hubspot.utils.FileUtils.writeFile(cacheFile, metadata.toString(4));
                    final HSFile file = process(note.getId(), fileId, metadata);
                    attachments.add(file);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new ArrayList<>(attachments);
    }

    private static HSFile process(final long engagementId, final long fileId, final JSONObject metadata) {
        if (metadata.has("type")) {
            final String type = metadata.getString("type");
            final String extension = metadata.getString("extension");
            final String name = metadata.getString("name");
            final long size = metadata.getLong("size");
            final boolean hidden = metadata.getBoolean("hidden");
            final String url = metadata.getString("url");
            switch (type) {
                case "DOCUMENT":
                    return new Document(fileId, engagementId, name, extension, size, hidden, url);
                case "IMG":
                case "TEXT":
                case "OTHER":
                    logger.debug(metadata.toString(4));
                default:
                    return null;
            }
        }
        else {
            return null;
        }
    }

    static HSFile getFileMetadata(final HttpService httpService,
                                  final RateLimiter rateLimiter,
                                  final long engagementId,
                                  final long fileId
    )
    throws HubSpotException {
        final String url = "/filemanager/api/v2/files/" + fileId;
        rateLimiter.acquire(1);
        final JSONObject metadata = (JSONObject) httpService.getRequest(url);
        return process(engagementId, fileId, metadata);
    }

    static List<HSFile> getFileMetadatas(final HttpService httpService,
                                         final RateLimiter rateLimiter,
                                         final Note note
    ) {
        final List<Long> attachmentIds = note.getAttachmentIds();
        final int capacity = (int) Math.ceil(Math.ceil((double) attachmentIds.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final Iterable<List<Long>> partitions = Iterables.partition(attachmentIds, LIMIT);
        final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                         STARTING_POOL_SIZE,
                                                                                         0L,
                                                                                         TimeUnit.MILLISECONDS,
                                                                                         new LinkedBlockingQueue<>(Math.max(
                                                                                                 capacity,
                                                                                                 Runtime.getRuntime()
                                                                                                        .availableProcessors()
                                                                                         )),
                                                                                         new CustomThreadFactory(
                                                                                                 "FileMetadataGrabber"),
                                                                                         new StoringRejectedExecutionHandler()
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("FileMetadataGrabberUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getFileMetadatas", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        final List<HSFile> attachments = Collections.synchronizedList(new ArrayList<>());
        for (final List<Long> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final Long fileId : partition) {
                    final String url = "/filemanager/api/v2/files/" + fileId;
                    rateLimiter.acquire(1);
                    final JSONObject metadata = (JSONObject) httpService.getRequest(url);
                    final HSFile file = process(note.getId(), fileId, metadata);
                    attachments.add(file);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new ArrayList<>(attachments);
    }
}
