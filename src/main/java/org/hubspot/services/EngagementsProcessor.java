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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class EngagementsProcessor {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final int WORDWRAP = 80;

    static EngagementData getAllEngagements(HttpService httpService, long contactId) throws HubSpotException {
        ArrayList<Long> engagementIdsToIterate = getAllEngagementIds(httpService, contactId);
        List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>(engagementIdsToIterate));
        List<Engagement> engagements = Collections.synchronizedList(new ArrayList<>());
        engagementIdsToIterate.parallelStream().forEach(engagementId -> {
            try {
                Engagement engagement = getEngagement(httpService, engagementId);
                if (engagement == null) {
                    engagementIds.remove(engagementId);
                } else {
                    engagements.add(engagement);
                }
            } catch (HubSpotException e) {
                logger.fatal("Unable to get engagement id " + engagementId + " for contact id " + contactId, e);

            }
        });
        return new EngagementData((ArrayList<Long>) engagementIds, (ArrayList<Engagement>) engagements);
    }

    static ArrayList<Long> getAllEngagementIds(HttpService httpService, long contactId) throws HubSpotException {
        String url = "/crm-associations/v1/associations/" + contactId + "/HUBSPOT_DEFINED/9";
        Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", 5);
        List<Long> engagementIds = Collections.synchronizedList(new ArrayList<>());
        long offset;
        ExecutorService executorService = Executors.newFixedThreadPool(10, new CustomThreadFactory(contactId + "_engagementIds"));
        try {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
                JSONArray jsonEngagementIds = jsonObject.getJSONArray("results");
                Runnable runnable = () -> {
                    for (int i = 0; i < jsonEngagementIds.length(); i++) {
                        engagementIds.add(jsonEngagementIds.getLong(i));
                    }
                    Utils.sleep(1L);
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
                throw new HubSpotException("Thread interrupted", e);
            }
            return (ArrayList<Long>) engagementIds;
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return new ArrayList<>();
            } else {
                throw e;
            }
        }
    }

    static Engagement getEngagement(HttpService service, long engagementId) throws HubSpotException {
        String engagementUrl = "/engagements/v1/engagements/" + engagementId;
        try {
            JSONObject jsonNote = (JSONObject) service.getRequest(engagementUrl);
            if (jsonNote == null) {
                throw new HubSpotException("Unable to grab engagement");
            }
            Engagement engagement = process(jsonNote);
            if (engagement == null) {
                JSONObject engagementJson = jsonNote.getJSONObject("engagement");
                String type = engagementJson.getString("type");
                if (!type.toLowerCase().contains("conversation")) {
                    throw new HubSpotException("Invalid engagement type");
                } else {
                    return null;
                }
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

    private static Engagement process(JSONObject engagementJson) {
        JSONObject engagementData = engagementJson.getJSONObject("engagement");
        String type = engagementData.getString("type");
        JSONObject engagementMetadata = engagementJson.getJSONObject("metadata");
        long id = engagementData.getLong("id");
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
                ArrayList<Email.Details> to = new ArrayList<>();
                ArrayList<Email.Details> cc = new ArrayList<>();
                ArrayList<Email.Details> bcc = new ArrayList<>();
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
                if (engagementMetadata.has("body")) {
                    note = engagementMetadata.get("body").toString();
                    note = Utils.format(note, WORDWRAP);
                }
                List<Long> attachments = new LinkedList<>();
                if (engagementJson.has("attachments")) {
                    JSONArray jsonAttachments = engagementJson.getJSONArray("attachments");
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
                if (engagementMetadata.has("endTime"))
                    meetingEndTime = engagementMetadata.getLong("endTime");
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
                List<Long> remindersMilliseconds = new LinkedList<>();
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
                    JSONArray jsonArray = engagementMetadata.getJSONArray("reminders");
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
            firstName = jsonDetails.get("firstName").toString();
        }
        if (jsonDetails.has("lastName")) {
            lastName = jsonDetails.get("lastName").toString();
        }
        if (jsonDetails.has("email")) {
            email = jsonDetails.get("email").toString();
        }
        return new Email.Details(firstName, lastName, email);
    }

    private static ArrayList<JSONObject> getEngagementJson(HttpService httpService, long contactId) throws HubSpotException {

        try {
            ArrayList<Long> engagementIdsToIterate = getAllEngagementIds(httpService, contactId);
            List<JSONObject> engagementJsons = Collections.synchronizedList(new ArrayList<>());
            engagementIdsToIterate.parallelStream().forEach(engagementId -> {
                String engagementUrl = "/engagements/v1/engagements/" + engagementId;
                try {
                    JSONObject jsonEngagement = (JSONObject) httpService.getRequest(engagementUrl);
                    if (jsonEngagement == null) {
                        throw new HubSpotException(new NullPointerException("Json Object is null"));
                    }
                    engagementJsons.add(jsonEngagement);
                } catch (HubSpotException e) {
                    logger.fatal("Unable to grab engagement", e);
                }
            });

            return (ArrayList<JSONObject>) engagementJsons;
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return new ArrayList<>();
            } else {
                throw e;
            }
        }
    }

    static void writeEngagementJson(HttpService httpService, long contactId) throws HubSpotException {

        Path folder = Paths.get("./cache/engagements/" + contactId + "/");
        try {
            Files.createDirectories(folder);
        } catch (IOException e) {
            throw new HubSpotException("Unable to write engagement jsons for contact id" + contactId, e);
        }
        String noteUrl = "/engagements/v1/engagements/" + contactId;
        File jsonFile = folder.resolve(contactId + ".json").toFile();
        try {
            JSONObject jsonNote = (JSONObject) httpService.getRequest(noteUrl);
            if (jsonNote == null) {
                throw new HubSpotException("Unable to grab engagement");
            } else {
                try {
                    FileWriter fileWriter = new FileWriter(jsonFile);
                    fileWriter.write(jsonNote.toString(4));
                    fileWriter.close();
                } catch (IOException e) {
                    throw new HubSpotException("Unable to write json");
                }
            }
        } catch (HubSpotException e) {
            if (!e.getMessage().equalsIgnoreCase("Not Found")) {
                throw e;
            }
        }
    }

    //static EngagementData readEngagementJsons()

    public static class EngagementData {
        private final ArrayList<Long> engagementIds;
        private final ArrayList<Engagement> engagements;

        public EngagementData(ArrayList<Long> engagementIds, ArrayList<Engagement> engagements) {
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
