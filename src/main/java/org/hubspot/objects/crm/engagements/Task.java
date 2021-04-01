package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Task {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final List<Date> reminders;
    private final String taskType;
    private final String subject;
    private final String forObjectType;
    private final String body;
    private final String status;
    private final Date completionDate;

    public Task(String taskType, String subject, String body, String forObjectType, String status, long completionDateMilliseconds, List<Long> remindersMilliseconds) {
        this.taskType = taskType;
        this.subject = subject;
        this.body = body;
        this.forObjectType = forObjectType;
        this.status = status;
        if (completionDateMilliseconds != -1) {
            this.completionDate = new Date(completionDateMilliseconds);
        } else {
            this.completionDate = null;
        }
        reminders = new LinkedList<>();
        for (long reminder : remindersMilliseconds) {
            reminders.add(new Date(reminder));
        }
    }

    public String getBody() {
        return body;
    }

    public Date getCompletionDate() {
        return completionDate;
    }

    public String getForObjectType() {
        return forObjectType;
    }

    public List<Date> getReminders() {
        return reminders;
    }

    public String getStatus() {
        return status;
    }

    public String getSubject() {
        return subject;
    }

    public String getTaskType() {
        return taskType;
    }

    @Override
    public String toString() {
        return "Task Type:\n" + taskType +
                "\nSubject:\n" + subject +
                "\nFor ObjectType:\n" + forObjectType +
                "\nStatus:\n" + status +
                "\nReminders:\n" + reminders +
                "\nCompletion Date:\n" + completionDate;
    }
}
