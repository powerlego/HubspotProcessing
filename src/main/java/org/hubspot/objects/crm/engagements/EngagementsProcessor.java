package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class EngagementsProcessor {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private static final int WORDWRAP = 80;

    public EngagementsProcessor() {

    }

    public static Object process(JSONObject jsonObject) {
        JSONObject engagement = jsonObject.getJSONObject("engagement");
        String type = engagement.getString("type");
        JSONObject metadata = jsonObject.getJSONObject("metadata");
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
                    to.add(jsonDetailsDetection(jsonDetails));
                }
                for (int i = 0; i < jsonCc.length(); i++) {
                    JSONObject jsonDetails = jsonCc.getJSONObject(i);
                    cc.add(jsonDetailsDetection(jsonDetails));
                }
                for (int i = 0; i < jsonBcc.length(); i++) {
                    JSONObject jsonDetails = jsonBcc.getJSONObject(i);
                    bcc.add(jsonDetailsDetection(jsonDetails));
                }
                Email.Details from = jsonDetailsDetection(jsonFrom);
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
                    return new Email(to, cc, bcc, from, emailSubject, builder.toString().strip());
                } else {
                    return new Email(to, cc, bcc, from, emailSubject, Utils.format(emailBody, WORDWRAP));
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
                return new Note(note, attachments);
            case "CALL":
                String callTitle = "";
                String callBody = "";
                if (metadata.has("title")) {
                    callTitle = metadata.getString("title");
                    callTitle = Utils.format(callTitle, WORDWRAP);
                }
                if (metadata.has("body")) {
                    callBody = metadata.getString("body");
                    callBody = Utils.format(callBody, WORDWRAP);
                }
                return new Call(callTitle, callBody);
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
                return new Meeting(meetingStartTime, meetingEndTime, meetingBody, meetingTitle);
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
                return new Task(taskType, taskSubject, taskBody, taskForObjectType, taskStatus, taskCompletionDateMilliseconds, remindersMilliseconds);
            default:
                return null;
        }
    }

    private static Email.Details jsonDetailsDetection(JSONObject jsonDetails) {
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

}
