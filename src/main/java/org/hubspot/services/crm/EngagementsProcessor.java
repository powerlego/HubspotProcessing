package org.hubspot.services.crm;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.*;
import org.hubspot.objects.crm.engagements.Email.Details;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.*;
import org.hubspot.utils.exceptions.HubSpotException;
import org.hubspot.utils.exceptions.NullException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class EngagementsProcessor {

    /**
     * The instance of the logger
     */
    private static final Logger      logger             = LogManager.getLogger();
    private static final int         WORDWRAP           = 80;
    private static final Path        cacheFolder        = Paths.get("./cache/engagements");
    private static final RateLimiter rateLimiter        = RateLimiter.create(13.0);
    private static final int         LIMIT              = 10;
    private static final long        UPDATE_INTERVAL    = 100;
    private static final int         STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final String      debugMessageFormat = "Method %-30s\tProcess Load: %f";
    private static final int         MAX_SIZE           = 50;

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static EngagementData getAllEngagements(final HttpService httpService, final long contactId)
    throws HubSpotException {
        final ArrayList<Long> engagementIdsToIterate = getAllEngagementIds(httpService, contactId);
        final Iterable<List<Long>> partitions = Iterables.partition(engagementIdsToIterate, LIMIT);
        final int capacity = (int) Math.ceil(Math.ceil((double) engagementIdsToIterate.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>(engagementIdsToIterate));
        final List<Engagement> engagements = Collections.synchronizedList(new ArrayList<>());
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (final IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        final Path folder = cacheFolder.resolve(contactId + "/");
        try {
            Files.createDirectories(folder);
        }
        catch (final IOException e) {
            logger.fatal("Unable to write engagement jsons for contact id {}", contactId, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
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
                                                                                               "EngagementGrabber_Contact_" +
                                                                                               contactId),
                                                                                       new StoringRejectedExecutionHandler(),
                                                                                       folder
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementGrabberUpdater_Contact_" +
                                                                                     contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getAllEngagements", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        for (final List<Long> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final Long engagementId : partition) {
                    final Engagement engagement = getEngagement(httpService, folder, engagementId);
                    if (engagement == null) {
                        engagementIds.remove(engagementId);
                    }
                    else {
                        engagements.add(engagement);
                    }
                    Utils.sleep(1);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new EngagementData(new ArrayList<>(engagementIds), new ArrayList<>(engagements));
    }

    static ArrayList<Long> getAllEngagementIds(final HttpService httpService, final long contactId)
    throws HubSpotException {
        final String url = "/crm-associations/v1/associations/" + contactId + "/HUBSPOT_DEFINED/9";
        final Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", LIMIT);
        final List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>());
        long offset;
        final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                         STARTING_POOL_SIZE,
                                                                                         0L,
                                                                                         TimeUnit.MILLISECONDS,
                                                                                         new LinkedBlockingQueue<>(200),
                                                                                         new CustomThreadFactory(
                                                                                                 "EngagementIds_Contact_" +
                                                                                                 contactId),
                                                                                         new StoringRejectedExecutionHandler()
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementIdsUpdater_Contact_" +
                                                                                     contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getAllEngagementIds", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        while (true) {
            rateLimiter.acquire(1);
            final JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
            final JSONArray jsonEngagementIds = jsonObject.getJSONArray("results");
            threadPoolExecutor.submit(() -> {
                for (int i = 0; i < jsonEngagementIds.length(); i++) {
                    engagementIds.add(jsonEngagementIds.getLong(i));
                    Utils.sleep(1);
                }
                return null;
            });
            if (!jsonObject.getBoolean("hasMore")) {
                break;
            }
            offset = jsonObject.getLong("offset");
            queryParam.put("offset", offset);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new ArrayList<>(engagementIds);
    }

    static Engagement getEngagement(final HttpService service, final Path folder, final long engagementId)
    throws HubSpotException {
        final String engagementUrl = "/engagements/v1/engagements/" + engagementId;
        rateLimiter.acquire(1);
        final JSONObject engagementJson = (JSONObject) service.getRequest(engagementUrl);
        if (engagementJson == null) {
            throw new HubSpotException(new NullException("Unable to grab engagement"),
                                       ErrorCodes.NULL_EXCEPTION.getErrorCode()
            );
        }
        final Engagement engagement = process(engagementJson);
        if (engagement == null) {
            final JSONObject engagementDataJson = engagementJson.getJSONObject("engagement");
            final String type = engagementDataJson.getString("type");
            if (!type.toLowerCase().contains("conversation")) {
                throw new HubSpotException("Invalid engagement type", ErrorCodes.INVALID_ENGAGEMENT.getErrorCode());
            }
            else {
                return null;
            }
        }
        else {
            try {
                FileUtils.writeJsonCache(folder, engagementJson);
            }
            catch (final IOException e) {
                throw new HubSpotException("Unable to write json to cache", ErrorCodes.IO_WRITE.getErrorCode(), e);
            }
            return engagement;
        }
    }

    private static Engagement process(final JSONObject engagementJson) {
        final JSONObject engagementData = engagementJson.getJSONObject("engagement");
        final String type = engagementData.getString("type");
        final JSONObject engagementMetadata = engagementJson.getJSONObject("metadata");
        final long id = engagementData.getLong("id");
        switch (type) {
            case "EMAIL":
            case "INCOMING_EMAIL":
            case "FORWARDED_EMAIL":
                JSONArray jsonTo = new JSONArray();
                JSONArray jsonCc = new JSONArray();
                JSONArray jsonBcc = new JSONArray();
                JSONObject jsonFrom = new JSONObject();
                String emailSubject = "";
                String emailBody = "";
                if (engagementMetadata.has("to")) {
                    jsonTo = engagementMetadata.getJSONArray("to");
                }
                if (engagementMetadata.has("cc")) {
                    jsonCc = engagementMetadata.getJSONArray("cc");
                }
                if (engagementMetadata.has("bcc")) {
                    jsonBcc = engagementMetadata.getJSONArray("bcc");
                }
                if (engagementMetadata.has("from")) {
                    jsonFrom = engagementMetadata.getJSONObject("from");
                }
                if (engagementMetadata.has("subject")) {
                    emailSubject = engagementMetadata.get("subject").toString();
                }
                if (engagementMetadata.has("text")) {
                    emailBody = engagementMetadata.get("text").toString();
                }
                final ArrayList<Details> to = new ArrayList<>();
                final ArrayList<Details> cc = new ArrayList<>();
                final ArrayList<Details> bcc = new ArrayList<>();
                for (int i = 0; i < jsonTo.length(); i++) {
                    final JSONObject jsonDetails = jsonTo.getJSONObject(i);
                    to.add(emailDetails(jsonDetails));
                }
                for (int i = 0; i < jsonCc.length(); i++) {
                    final JSONObject jsonDetails = jsonCc.getJSONObject(i);
                    cc.add(emailDetails(jsonDetails));
                }
                for (int i = 0; i < jsonBcc.length(); i++) {
                    final JSONObject jsonDetails = jsonBcc.getJSONObject(i);
                    bcc.add(emailDetails(jsonDetails));
                }
                final Details from = emailDetails(jsonFrom);
                emailBody = emailBody.replaceAll("\r", "");
                if (emailBody.contains("From:")) {
                    final String[] bodySplit = emailBody.split("From:");
                    final StringBuilder builder = new StringBuilder().append(Utils.format(bodySplit[0], WORDWRAP))
                                                                     .append("\n");
                    for (int i = 1; i < bodySplit.length; i++) {
                        builder.append(Utils.createLineDivider(WORDWRAP));
                        final String rest = "From:" + bodySplit[i];
                        builder.append(Utils.format(rest, WORDWRAP)).append("\n");
                    }
                    return new Email(id, to, cc, bcc, from, emailSubject, builder.toString().strip());
                }
                else {
                    return new Email(id, to, cc, bcc, from, emailSubject, Utils.format(emailBody, WORDWRAP));
                }
            case "NOTE":
                String note = "";
                if (engagementMetadata.has("body")) {
                    note = engagementMetadata.get("body").toString();
                    note = Utils.format(note, WORDWRAP);
                }
                final List<Long> attachments = new LinkedList<>();
                if (engagementJson.has("attachments")) {
                    final JSONArray jsonAttachments = engagementJson.getJSONArray("attachments");
                    for (int i = 0; i < jsonAttachments.length(); i++) {
                        final JSONObject jsonAttachment = jsonAttachments.getJSONObject(i);
                        final long attachment = jsonAttachment.getLong("id");
                        attachments.add(attachment);
                    }
                }
                return new Note(id, note, attachments);
            case "CALL":
                String callTitle = "";
                String callBody = "";
                String toNumber = "";
                String fromNumber = "";
                long durationMillis = 0;
                String recordingURL = "";
                if (engagementMetadata.has("title")) {
                    callTitle = engagementMetadata.get("title").toString();
                    callTitle = Utils.format(callTitle, WORDWRAP);
                }
                if (engagementMetadata.has("body")) {
                    callBody = engagementMetadata.get("body").toString();
                    callBody = Utils.format(callBody, WORDWRAP);
                }
                if (engagementMetadata.has("toNumber")) {
                    toNumber = engagementMetadata.get("toNumber").toString();
                }
                if (engagementMetadata.has("fromNumber")) {
                    fromNumber = engagementMetadata.get("fromNumber").toString();
                }
                if (engagementMetadata.has("durationMilliseconds")) {
                    durationMillis = engagementMetadata.getLong("durationMilliseconds");
                }
                if (engagementMetadata.has("recordingUrl")) {
                    recordingURL = engagementMetadata.get("recordingUrl").toString();
                }
                return new Call(id, callTitle, callBody, toNumber, fromNumber, durationMillis, recordingURL);
            case "MEETING":
                long meetingStartTime = -1;
                long meetingEndTime = -1;
                String meetingBody = "";
                String meetingTitle = "";
                if (engagementMetadata.has("startTime")) {
                    meetingStartTime = engagementMetadata.getLong("startTime");
                }
                if (engagementMetadata.has("endTime")) {
                    meetingEndTime = engagementMetadata.getLong("endTime");
                }
                if (engagementMetadata.has("body")) {
                    meetingBody = engagementMetadata.get("body").toString();
                    meetingBody = Utils.format(meetingBody, WORDWRAP);
                }
                if (engagementMetadata.has("title")) {
                    meetingTitle = engagementMetadata.get("title").toString();
                    meetingTitle = Utils.format(meetingTitle, WORDWRAP);
                }
                return new Meeting(id, meetingStartTime, meetingEndTime, meetingBody, meetingTitle);
            case "TASK":
                String taskType = "";
                String taskSubject = "";
                String taskForObjectType = "";
                String taskBody = "";
                String taskStatus = "";
                long taskCompletionDateMilliseconds = -1;
                final List<Long> remindersMilliseconds = new LinkedList<>();
                if (engagementMetadata.has("taskType")) {
                    taskType = engagementMetadata.get("taskType").toString();
                }
                if (engagementMetadata.has("subject")) {
                    taskSubject = engagementMetadata.get("subject").toString();
                    taskSubject = Utils.format(taskSubject, WORDWRAP);
                }
                if (engagementMetadata.has("forObjectType")) {
                    taskForObjectType = engagementMetadata.get("forObjectType").toString();
                }
                if (engagementMetadata.has("body")) {
                    taskBody = engagementMetadata.get("body").toString();
                    taskBody = Utils.format(taskBody, WORDWRAP);
                }
                if (engagementMetadata.has("status")) {
                    taskStatus = engagementMetadata.get("status").toString();
                }
                if (engagementMetadata.has("completionDate")) {
                    taskCompletionDateMilliseconds = engagementMetadata.getLong("completionDate");
                }
                if (engagementMetadata.has("reminders")) {
                    final JSONArray jsonArray = engagementMetadata.getJSONArray("reminders");
                    for (int i = 0; i < jsonArray.length(); i++) {
                        remindersMilliseconds.add(jsonArray.getLong(i));
                    }
                }
                return new Task(id,
                                taskType,
                                taskSubject,
                                taskBody,
                                taskForObjectType,
                                taskStatus,
                                taskCompletionDateMilliseconds,
                                remindersMilliseconds
                );
            default:
                return null;
        }
    }

    private static Details emailDetails(final JSONObject jsonDetails) {
        String firstName = "";
        String lastName = "";
        String email = "";
        if (jsonDetails.has("firstName")) {
            firstName = jsonDetails.get("firstName").toString();
        }
        if (jsonDetails.has("lastName")) {
            lastName = jsonDetails.get("lastName").toString();
        }
        if (jsonDetails.has("email")) {
            email = jsonDetails.get("email").toString();
        }
        return new Details(firstName, lastName, email);
    }

    public static Path getCacheFolder() {
        return cacheFolder;
    }

    static EngagementData getUpdatedEngagements(final HttpService httpService,
                                                final long contactId,
                                                final long lastFinished
    )
    throws HubSpotException {
        final Path folder = cacheFolder.resolve(contactId + "/");
        final ArrayList<Long> engagementIdsToIterate = getAllEngagementIds(httpService, contactId);
        final Iterable<List<Long>> partitions = Iterables.partition(engagementIdsToIterate, LIMIT);
        final int capacity = (int) Math.ceil(Math.ceil((double) engagementIdsToIterate.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>(engagementIdsToIterate));
        final List<Engagement> engagements = Collections.synchronizedList(new ArrayList<>());
        final UpdateThreadPoolExecutor threadPoolExecutor = new UpdateThreadPoolExecutor(1,
                                                                                         STARTING_POOL_SIZE,
                                                                                         0L,
                                                                                         TimeUnit.MILLISECONDS,
                                                                                         new LinkedBlockingQueue<>(Math.max(
                                                                                                 capacity,
                                                                                                 Runtime.getRuntime()
                                                                                                        .availableProcessors()
                                                                                         )),
                                                                                         new CustomThreadFactory(
                                                                                                 "EngagementUpdateGrabber_Contact_" +
                                                                                                 contactId),
                                                                                         new StoringRejectedExecutionHandler(),
                                                                                         folder,
                                                                                         lastFinished
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory(
                "EngagementUpdateGrabberUpdater_Contact_" + contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getUpdatedEngagements", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        for (final List<Long> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final Long engagementId : partition) {
                    final Path file = folder.resolve(engagementId + ".json");
                    if (!file.toFile().exists()) {
                        final Engagement engagement = getEngagement(httpService, folder, engagementId);
                        if (engagement == null) {
                            engagementIds.remove(engagementId);
                        }
                        else {
                            engagements.add(engagement);
                        }
                    }
                    else {
                        engagementIds.remove(engagementId);
                    }
                    Utils.sleep(1);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new EngagementData(new ArrayList<>(engagementIds), new ArrayList<>(engagements));
    }

    static HashMap<Long, EngagementData> readEngagementJsons() throws HubSpotException {
        final ConcurrentHashMap<Long, EngagementData> contactsEngagementData = new ConcurrentHashMap<>();
        final File[] files = cacheFolder.toFile().listFiles(File::isDirectory);
        if (files != null) {
            final List<File> fileList = Arrays.asList(files);
            final Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            final int capacity = (int) Math.ceil(Math.ceil((double) fileList.size() / (double) LIMIT) *
                                                 Math.pow(MAX_SIZE, -0.6));
            final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
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
                                                                                                     "EngagementsReader"),
                                                                                             new StoringRejectedExecutionHandler()
            );
            final ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementsReaderUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                final double load = CPUMonitor.getProcessLoad();
                final String debugMessage = String.format(debugMessageFormat, "readEngagementJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            final ProgressBar pb = Utils.createProgressBar("Reading Engagement Cache", fileList.size());
            for (final List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final File file : partition) {
                        final long contactId = Long.parseLong(file.getName());
                        final EngagementData engagementData = readContactEngagementJsons(file, contactId);
                        contactsEngagementData.put(contactId, engagementData);
                        pb.step();
                        Utils.sleep(1);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
            pb.close();
        }
        return new HashMap<>(contactsEngagementData);
    }

    private static EngagementData readContactEngagementJsons(final File contactFolder, final long contactId) {
        final File[] files = contactFolder.listFiles();
        final List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>());
        final List<Engagement> engagements = Collections.synchronizedList(new ArrayList<>());
        if (files != null) {
            final List<File> fileList = Arrays.asList(files);
            final Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            final int capacity = (int) Math.ceil(Math.ceil((double) fileList.size() / (double) LIMIT) *
                                                 Math.pow(MAX_SIZE, -0.6));
            final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
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
                                                                                                     "EngagementReader_Contact_" +
                                                                                                     contactId),
                                                                                             new StoringRejectedExecutionHandler()
            );
            final ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory(
                    "EngagementReaderUpdater_Contact_" + contactId));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                final double load = CPUMonitor.getProcessLoad();
                final String debugMessage = String.format(debugMessageFormat, "readContactEngagementJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            for (final List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final File file : partition) {
                        final String jsonString = FileUtils.readJsonString(logger, file);
                        final JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                        final long engagementId = jsonObject.getJSONObject("engagement").getLong("id");
                        final Engagement engagement = process(jsonObject);
                        engagementIds.add(engagementId);
                        engagements.add(engagement);
                        Utils.sleep(1);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
        }
        return new EngagementData(new ArrayList<>(engagementIds), new ArrayList<>(engagements));
    }

    static void writeContactEngagementJsons(final HttpService httpService, final long contactId)
    throws HubSpotException {
        final Path folder = cacheFolder.resolve(contactId + "/");
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (final IOException e) {
            throw new HubSpotException("Unable to create cache directory " + cacheFolder,
                                       ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode(),
                                       e
            );
        }
        try {
            Files.createDirectories(folder);
        }
        catch (final IOException e) {
            throw new HubSpotException("Unable to write engagement jsons for contact id" + contactId,
                                       ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode(),
                                       e
            );
        }
        final ArrayList<JSONObject> engagementJsons = getEngagementJson(httpService, contactId);
        final Iterable<List<JSONObject>> partitions = Iterables.partition(engagementJsons, LIMIT);
        final int capacity = (int) Math.ceil(Math.ceil((double) engagementJsons.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
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
                                                                                               "EngagementJsons_Contact_" +
                                                                                               contactId),
                                                                                       new StoringRejectedExecutionHandler(),
                                                                                       folder
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("EngagementJsonsUpdater_Contact_" +
                                                                                     contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "writeContactEngagementJsons", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        final ProgressBar pb = Utils.createProgressBar("Writing Engagements for Contact ID " + contactId,
                                                       engagementJsons.size()
        );
        for (final List<JSONObject> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (final JSONObject jsonObject : partition) {
                    FileUtils.writeJsonCache(folder, jsonObject);
                    pb.step();
                    Utils.sleep(1);
                }
                return null;
            });
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
    }

    private static ArrayList<JSONObject> getEngagementJson(final HttpService httpService, final long contactId)
    throws HubSpotException {
        try {
            final ArrayList<Long> engagementIdsToIterate = getAllEngagementIds(httpService, contactId);
            final Iterable<List<Long>> partitions = Iterables.partition(engagementIdsToIterate, LIMIT);
            final int capacity = (int) Math.ceil(Math.ceil((double) engagementIdsToIterate.size() / (double) LIMIT) *
                                                 Math.pow(MAX_SIZE, -0.6));
            final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
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
                                                                                                     "EngagementJsonGrabber_Contact_" +
                                                                                                     contactId),
                                                                                             new StoringRejectedExecutionHandler()
            );
            final ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory(
                    "EngagementJsonGrabberUpdater_Contact_" + contactId));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                final double load = CPUMonitor.getProcessLoad();
                final String debugMessage = String.format(debugMessageFormat, "getEngagementJson", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            final List<JSONObject> engagementJsons = Collections.synchronizedList(new ArrayList<>());
            final ProgressBar pb = Utils.createProgressBar("Grabbing Engagement Jsons for Contact ID" + contactId,
                                                           engagementIdsToIterate.size()
            );
            for (final List<Long> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final long engagementId : partition) {
                        final String engagementUrl = "/engagements/v1/engagements/" + engagementId;
                        rateLimiter.acquire();
                        final JSONObject jsonEngagement = (JSONObject) httpService.getRequest(engagementUrl);
                        if (jsonEngagement == null) {
                            throw new HubSpotException(new NullException("Json Object is null"),
                                                       ErrorCodes.NULL_EXCEPTION.getErrorCode()
                            );
                        }
                        engagementJsons.add(jsonEngagement);
                        pb.step();
                        Utils.sleep(1);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
            pb.close();
            return new ArrayList<>(engagementJsons);
        }
        catch (final HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return new ArrayList<>();
            }
            else {
                throw e;
            }
        }
    }

    public static class EngagementData {

        private final ArrayList<Long>       engagementIds;
        private final ArrayList<Engagement> engagements;

        public EngagementData(final ArrayList<Long> engagementIds, final ArrayList<Engagement> engagements) {
            this.engagementIds = engagementIds;
            this.engagements = engagements;
        }

        public ArrayList<Long> getEngagementIds() {
            return engagementIds;
        }

        public ArrayList<Engagement> getEngagements() {
            return engagements;
        }
    }
}
