package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicholas Curl
 */
public class Call extends Engagement {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final String title;
    private final String body;
    private final String recordingURL;
    private final String duration;
    private final String toNumber;
    private final String fromNumber;

    public Call(long id,
                String title,
                String body,
                String toNumber,
                String fromNumber,
                long durationMilliseconds,
                String recordingURL
    ) {
        super(id);
        this.title = title;
        this.body = body;
        this.recordingURL = recordingURL;
        this.toNumber = toNumber;
        this.fromNumber = fromNumber;
        long millis = durationMilliseconds % 1000;
        long second = (durationMilliseconds / 1000) % 60;
        long minute = (durationMilliseconds / (1000 * 60)) % 60;
        long hour = (durationMilliseconds / (1000 * 60 * 60)) % 24;
        duration = String.format("%02d:%02d:%02d.%d", hour, minute, second, millis);
    }

    public String getBody() {
        return body;
    }

    public String getDuration() {
        return duration;
    }

    public String getFromNumber() {
        return fromNumber;
    }

    public String getRecordingURL() {
        return recordingURL;
    }

    public String getTitle() {
        return title;
    }

    public String getToNumber() {
        return toNumber;
    }

    @Override
    public String toString() {
        return "To:\n" + toNumber +
                "From:\n" + fromNumber +
                "\nTitle:\n" + title +
                "\nBody:\n" + body +
                "\nDuration:\n" + duration +
                "Recording URL:\n" + recordingURL;
    }
}
