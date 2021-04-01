package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;

/**
 * @author Nicholas Curl
 */
public class Meeting {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final Date startTime;
    private final Date endTime;
    private final String body;
    private final String title;

    public Meeting(long startTime, long endTime, String body, String title) {
        if (startTime != -1) {
            this.startTime = new Date(startTime);
        } else {
            this.startTime = null;
        }
        if (endTime != -1) {
            this.endTime = new Date(endTime);
        } else {
            this.endTime = null;
        }
        this.body = body;
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public Date getEndTime() {
        return endTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public String getTitle() {
        return title;
    }

    @Override
    public String toString() {
        return "Title:\t" + title +
                "\nStart Time:\t" + startTime +
                "\nEnd Time:\t" + endTime +
                "\nBody:\n" + body;
    }
}
