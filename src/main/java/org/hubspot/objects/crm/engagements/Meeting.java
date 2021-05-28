package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;

/**
 * A class that represents a meeting engagement
 *
 * @author Nicholas Curl
 */
public class Meeting extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(Meeting.class);
    /**
     * The serial version UID for this class
     */
    private static final long   serialVersionUID = 5657344402502100558L;
    /**
     * The start date of this meeting
     */
    private final        Date   startTime;
    /**
     * The end date of this meeting
     */
    private final        Date   endTime;
    /**
     * The body/description of the meeting
     */
    private final        String body;
    /**
     * The title for this meeting
     */
    private final        String title;

    /**
     * A constructor for a Meeting object
     *
     * @param id        The engagement id
     * @param startTime The start time of the meeting in milliseconds since January 1, 1970, 00:00:00 GMT
     * @param endTime   The end time of the meeting in milliseconds since January 1, 1970, 00:00:00 GMT
     * @param body      The body/description of the meeting
     * @param title     The title of the meeting
     */
    public Meeting(long id, long startTime, long endTime, String body, String title) {
        super(id, EngagementType.MEETING);
        if (startTime != -1) {
            this.startTime = new Date(startTime);
        }
        else {
            this.startTime = null;
        }
        if (endTime != -1) {
            this.endTime = new Date(endTime);
        }
        else {
            this.endTime = null;
        }
        this.body = body;
        this.title = title;
    }

    /**
     * Gets the body/description of this meeting
     *
     * @return The body/description of this meeting
     */
    public String getBody() {
        return body;
    }

    /**
     * Gets the end date of this meeting
     *
     * @return The end date of this meeting
     */
    public Date getEndTime() {
        return endTime;
    }

    /**
     * Gets the start date of this meeting
     *
     * @return The start date of this meeting
     */
    public Date getStartTime() {
        return startTime;
    }

    /**
     * Gets the title for this meeting
     *
     * @return The title for this meeting
     */
    public String getTitle() {
        return title;
    }

    /**
     * Returns the string representation of the meeting
     *
     * @return The string representation of the meeting
     */
    @Override
    public String toString() {
        return "Title:\t" + title + "\nStart Time:\t" + startTime + "\nEnd Time:\t" + endTime + "\nBody:\n" + body;
    }
}
