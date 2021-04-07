package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.*;
import org.hubspot.utils.CustomThreadFactory;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Nicholas Curl
 */
public class EngagementsProcessor {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final int WORDWRAP = 80;

    static List<Long> getAllEngagementIds(HttpService httpService, long id) throws HubSpotException {
        String url = "/crm-associations/v1/associations/" + id + "/HUBSPOT_DEFINED/9";
        Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", 10);
        List<Long> notes = Collections.synchronizedList(new LinkedList<>());
        long offset;
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(10, new CustomThreadFactory(id + "_engagementIds"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = () -> Utils.getCompleted(logger, completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        try {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
                JSONArray ids = jsonObject.getJSONArray("results");
                Runnable runnable = () -> {
                    for (int i = 0; i < ids.length(); i++) {
                        notes.add(ids.getLong(i));
                    }
                };
                executorService.submit(runnable);
                if (!jsonObject.getBoolean("hasMore")) {
                    break;
                }
                offset = jsonObject.getLong("offset");
                queryParam.put("offset", offset);
                Utils.sleep(500L);
            }
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    logger.warn("Termination Timeout");
                }
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted", e);
            }
            return notes;
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }


    static EngagementData getAllEngagements(HttpService httpService, long id) throws HubSpotException {
        String url = "/crm-associations/v1/associations/" + id + "/HUBSPOT_DEFINED/9";
        Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", 10);
        List<Long> ids = Collections.synchronizedList(new LinkedList<>());
        List<Engagement> engagements = Collections.synchronizedList(new LinkedList<>());
        long offset;
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(10, new CustomThreadFactory(id + "_engagements"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        String idString = String.valueOf(id);
        int width = WORDWRAP - idString.length();
        int left = (width / 2) - 1;
        int right = ((width / 2) + (width % 2)) - 1;
        String separator = "\n" + "-".repeat(left) + "[" + idString + "]" + "-".repeat(right);
        logger.debug(separator);
        long startTime = System.nanoTime();
        Runnable progress = () -> Utils.getCompleted(logger, completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        try {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
                JSONArray jsonNotes = jsonObject.getJSONArray("results");
                Runnable runnable = () -> {
                    for (int i = 0; i < jsonNotes.length(); i++) {
                        long engagementId = jsonNotes.getLong(i);
                        try {
                            ids.add(engagementId);
                            engagements.add(getEngagement(httpService, engagementId));
                            completed.getAndIncrement();
                        } catch (HubSpotException e) {
                            logger.warn("Unable to get engagement id " + engagementId + " for contact id " + id, e);
                        }
                    }
                };
                executorService.submit(runnable);
                if (!jsonObject.getBoolean("hasMore")) {
                    break;
                }
                offset = jsonObject.getLong("offset");
                queryParam.put("offset", offset);
                Utils.sleep(500L);
            }
            Utils.shutdownExecutors(logger, executorService, completed, scheduledExecutorService, startTime);
            return new EngagementData(ids, engagements);
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }

    static Engagement getEngagement(HttpService service, long id) throws HubSpotException {
        String noteUrl = "/engagements/v1/engagements/" + id;
        try {
            JSONObject jsonNote = (JSONObject) service.getRequest(noteUrl);
            if (jsonNote == null) {
                throw new HubSpotException("Unable to grab engagement");
            }
            Engagement engagement = process(jsonNote);
            if (engagement == null) {
                throw new HubSpotException("Invalid engagement type");
            } else {
                return engagement;
            }
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }

    private static Engagement process(JSONObject jsonObject) {
        JSONObject engagement = jsonObject.getJSONObject("engagement");
        String type = engagement.getString("type");
        JSONObject metadata = jsonObject.getJSONObject("metadata");
        long id = engagement.getLong("id");
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
                if (metadata.has("to")) {
                    jsonTo = metadata.getJSONArray("to");
                }
                if (metadata.has("cc")) {
                    jsonCc = metadata.getJSONArray("cc");
                }
                if (metadata.has("bcc")) {
                    jsonBcc = metadata.getJSONArray("bcc");
                }
                if (metadata.has("from")) {
                    jsonFrom = metadata.getJSONObject("from");
                }
                if (metadata.has("subject")) {
                    emailSubject = metadata.getString("subject");
                }
                if (metadata.has("text")) {
                    emailBody = metadata.getString("text");
                }
                List<Email.Details> to = new LinkedList<>();
                List<Email.Details> cc = new LinkedList<>();
                List<Email.Details> bcc = new LinkedList<>();
                for (int i = 0; i < jsonTo.length(); i++) {
                    JSONObject jsonDetails = jsonTo.getJSONObject(i);
                    to.add(emailDetails(jsonDetails));
                }
                for (int i = 0; i < jsonCc.length(); i++) {
                    JSONObject jsonDetails = jsonCc.getJSONObject(i);
                    cc.add(emailDetails(jsonDetails));
                }
                for (int i = 0; i < jsonBcc.length(); i++) {
                    JSONObject jsonDetails = jsonBcc.getJSONObject(i);
                    bcc.add(emailDetails(jsonDetails));
                }
                Email.Details from = emailDetails(jsonFrom);
                emailBody = emailBody.replaceAll("\r", "");
                if (emailBody.contains("From:")) {
                    String[] bodySplit = emailBody.split("From:");
                    StringBuilder builder = new StringBuilder();
                    builder.append(Utils.format(bodySplit[0], WORDWRAP)).append("\n");
                    for (int i = 1; i < bodySplit.length; i++) {
                        builder.append(Utils.createLineDivider(WORDWRAP));
                        String rest = "From:" + bodySplit[i];
                        builder.append(Utils.format(rest, WORDWRAP)).append("\n");
                    }
                    return new Email(id, to, cc, bcc, from, emailSubject, builder.toString().strip());
                } else {
                    return new Email(id, to, cc, bcc, from, emailSubject, Utils.format(emailBody, WORDWRAP));
                }
            case "NOTE":
                String note = "";
                if (metadata.has("body")) {
                    note = metadata.getString("body");
                    note = Utils.format(note, WORDWRAP);
                }
                List<Long> attachments = new LinkedList<>();
                if (jsonObject.has("attachments")) {
                    JSONArray jsonAttachments = jsonObject.getJSONArray("attachments");
                    for (int i = 0; i < jsonAttachments.length(); i++) {
                        JSONObject jsonAttachment = jsonAttachments.getJSONObject(i);
                        long attachment = jsonAttachment.getLong("id");
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
                if (metadata.has("title")) {
                    callTitle = metadata.getString("title");
                    callTitle = Utils.format(callTitle, WORDWRAP);
                }
                if (metadata.has("body")) {
                    callBody = metadata.getString("body");
                    callBody = Utils.format(callBody, WORDWRAP);
                }
                if (metadata.has("toNumber")) {
                    toNumber = metadata.getString("toNumber");
                }
                if (metadata.has("fromNumber")) {
                    fromNumber = metadata.getString("fromNumber");
                }
                if (metadata.has("durationMilliseconds")) {
                    durationMillis = metadata.getLong("durationMilliseconds");
                }
                if (metadata.has("recordingUrl")) {
                    recordingURL = metadata.getString("recordingUrl");
                }
                return new Call(id, callTitle, callBody, toNumber, fromNumber, durationMillis, recordingURL);
            case "MEETING":
                long meetingStartTime = -1;
                long meetingEndTime = -1;
                String meetingBody = "";
                String meetingTitle = "";
                if (metadata.has("startTime")) {
                    meetingStartTime = metadata.getLong("startTime");
                }
                if (metadata.has("endTime"))
                    meetingEndTime = metadata.getLong("endTime");
                if (metadata.has("body")) {
                    meetingBody = metadata.getString("body");
                    meetingBody = Utils.format(meetingBody, WORDWRAP);
                }
                if (metadata.has("title")) {
                    meetingTitle = metadata.getString("title");
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
                List<Long> remindersMilliseconds = new LinkedList<>();
                if (metadata.has("taskType")) {
                    taskType = metadata.getString("taskType");
                }
                if (metadata.has("subject")) {
                    taskSubject = metadata.getString("subject");
                    taskSubject = Utils.format(taskSubject, WORDWRAP);
                }
                if (metadata.has("forObjectType")) {
                    taskForObjectType = metadata.getString("forObjectType");
                }
                if (metadata.has("body")) {
                    taskBody = metadata.getString("body");
                    taskBody = Utils.format(taskBody, WORDWRAP);
                }
                if (metadata.has("status")) {
                    taskStatus = metadata.getString("status");
                }
                if (metadata.has("completionDate")) {
                    taskCompletionDateMilliseconds = metadata.getLong("completionDate");
                }
                if (metadata.has("reminders")) {
                    JSONArray jsonArray = metadata.getJSONArray("reminders");
                    for (int i = 0; i < jsonArray.length(); i++) {
                        remindersMilliseconds.add(jsonArray.getLong(i));
                    }
                }
                return new Task(id, taskType, taskSubject, taskBody, taskForObjectType, taskStatus, taskCompletionDateMilliseconds, remindersMilliseconds);
            default:
                return null;
        }
    }

    private static Email.Details emailDetails(JSONObject jsonDetails) {
        String firstName = "";
        String lastName = "";
        String email = "";
        if (jsonDetails.has("firstName")) {
            firstName = jsonDetails.getString("firstName");
        }
        if (jsonDetails.has("lastName")) {
            lastName = jsonDetails.getString("lastName");
        }
        if (jsonDetails.has("email")) {
            email = jsonDetails.getString("email");
        }
        return new Email.Details(firstName, lastName, email);
    }

    public static class EngagementData {
        private final List<Long> engagementIds;
        private final List<Engagement> engagements;

        public EngagementData(List<Long> engagementIds, List<Engagement> engagements) {
            this.engagementIds = engagementIds;
            this.engagements = engagements;
        }

        public List<Long> getEngagementIds() {
            return engagementIds;
        }

        public List<Engagement> getEngagements() {
            return engagements;
        }
    }
}
