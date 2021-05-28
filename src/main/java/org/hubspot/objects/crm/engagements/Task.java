package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * A class that represents a task engagement
 *
 * @author Nicholas Curl
 */
public class Task extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger     logger           = LogManager.getLogger(Task.class);
    /**
     * The serial version UID for this class
     */
    private static final long       serialVersionUID = -514871948205948346L;
    /**
     * List of reminder dates
     */
    private final        List<Date> reminders;
    /**
     * The type of task
     */
    private final        String     taskType;
    /**
     * The subject of the task
     */
    private final        String     subject;
    /**
     * The Hubspot CRM Object type that this task is associated with
     */
    private final        String     forObjectType;
    /**
     * The body/description of the task
     */
    private final        String     body;
    /**
     * The status of the task
     */
    private final        String     status;
    /**
     * The date that this task was completed
     */
    private final        Date       completionDate;

    /**
     * A constructor for a Task engagement
     *
     * @param id                         The engagement type
     * @param taskType                   The type of task
     * @param subject                    The subject of the task
     * @param body                       The body/description of the task
     * @param forObjectType              The Hubspot CRM Object type that this task is associated with
     * @param status                     The status of the task
     * @param completionDateMilliseconds The date that this task was completed in milliseconds since January 1, 1970,
     *                                   00:00:00 GMT
     * @param remindersMilliseconds      The list of reminders for the task in milliseconds since January 1, 1970,
     *                                   00:00:00 GMT
     */
    public Task(long id,
                String taskType,
                String subject,
                String body,
                String forObjectType,
                String status,
                long completionDateMilliseconds,
                List<Long> remindersMilliseconds
    ) {
        super(id, EngagementType.TASK);
        this.taskType = taskType;
        this.subject = subject;
        this.body = body;
        this.forObjectType = forObjectType;
        this.status = status;
        if (completionDateMilliseconds != -1) {
            this.completionDate = new Date(completionDateMilliseconds);
        }
        else {
            this.completionDate = null;
        }
        reminders = new LinkedList<>();
        for (long reminder : remindersMilliseconds) {
            reminders.add(new Date(reminder));
        }
    }

    /**
     * Gets the body/description for the task
     *
     * @return The body/description for the task
     */
    public String getBody() {
        return body;
    }

    /**
     * Gets the date that this task was completed on
     *
     * @return The date that this task was completed on
     */
    public Date getCompletionDate() {
        return completionDate;
    }

    /**
     * Gets the Hubspot CRM Object type associated with this task
     *
     * @return The Hubspot CRM Object type associated with this task
     */
    public String getForObjectType() {
        return forObjectType;
    }

    /**
     * Gets the list of reminder dates for this task
     *
     * @return The list of reminder dates for this task
     */
    public List<Date> getReminders() {
        return reminders;
    }

    /**
     * Gets the status of this task
     *
     * @return The status of this task
     */
    public String getStatus() {
        return status;
    }

    /**
     * Gets the subject of this task
     *
     * @return The subject of this task
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Gets the type of task for this task
     *
     * @return The type of task for this task
     */
    public String getTaskType() {
        return taskType;
    }

    /**
     * Returns the string representation of this task
     *
     * @return The string representation of this task
     */
    @Override
    public String toString() {
        return "Task Type:\n" +
               taskType +
               "\nSubject:\n" +
               subject +
               "\nFor ObjectType:\n" +
               forObjectType +
               "\nStatus:\n" +
               status +
               "\nReminders:\n" +
               reminders +
               "\nCompletion Date:\n" +
               completionDate;
    }
}
