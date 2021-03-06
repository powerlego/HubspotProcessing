package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class to represent a call engagement
 *
 * @author Nicholas Curl
 */
public class Call extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(Call.class);
    /**
     * The serial version UID for this class
     */
    private static final long   serialVersionUID = 3521110114441120931L;
    /**
     * The title of the call
     */
    private final        String title;
    /**
     * The description/body of the call
     */
    private final        String body;
    /**
     * The url that contains a recording of the call
     */
    private final        String recordingURL;
    /**
     * The duration of the call
     */
    private final        String duration;
    private final        long   durationMillis;
    /**
     * The phone number that was being called
     */
    private final        String toNumber;
    /**
     * The phone number that was placing the call
     */
    private final        String fromNumber;

    /**
     * A constructor for a Call object
     *
     * @param id                   The Hubspot engagement id
     * @param title                The title of the call
     * @param body                 The description/body of the call
     * @param toNumber             The phone number that was being called
     * @param fromNumber           The phone number that was placing the call
     * @param durationMilliseconds The call duration in milliseconds
     * @param recordingURL         The url of the call recording
     */
    public Call(long id,
                String title,
                String body,
                String toNumber,
                String fromNumber,
                long durationMilliseconds,
                String recordingURL
    ) {
        super(id, EngagementType.CALL);
        logger.traceEntry("id={}, title={}, toNumber={}, fromNumber={}, durationMilliseconds={}, recordingUrl={}",
                          id,
                          title,
                          toNumber,
                          fromNumber,
                          durationMilliseconds,
                          recordingURL
        );
        this.title = title;
        this.body = body;
        this.recordingURL = recordingURL;
        this.toNumber = toNumber;
        this.fromNumber = fromNumber;
        this.durationMillis = durationMilliseconds;
        long millis = durationMilliseconds % 1000;
        long second = (durationMilliseconds / 1000) % 60;
        long minute = (durationMilliseconds / (1000 * 60)) % 60;
        long hour = (durationMilliseconds / (1000 * 60 * 60)) % 24;
        duration = String.format("%02d:%02d:%02d.%d", hour, minute, second, millis);
        logger.traceExit();
    }

    /**
     * Gets the description/body of the call
     *
     * @return The description/body of the call
     */
    public String getBody() {
        return body;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    /**
     * Gets the call's duration
     *
     * @return The call's duration
     */
    public String getDuration() {
        return duration;
    }

    /**
     * Gets the phone number that was placing the call
     *
     * @return The phone number that was placing the call
     */
    public String getFromNumber() {
        return fromNumber;
    }

    /**
     * Gets the url of the call's recording
     *
     * @return The url of the call's recording
     */
    public String getRecordingURL() {
        return recordingURL;
    }

    /**
     * Gets the call's title
     *
     * @return The call's title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Gets the phone number that was being called
     *
     * @return The phone number that was being called
     */
    public String getToNumber() {
        return toNumber;
    }

    /**
     * Returns the string representation of this call
     *
     * @return The string representation of this call
     */
    @Override
    public String toString() {
        return "To:\n" +
               toNumber +
               "From:\n" +
               fromNumber +
               "\nTitle:\n" +
               title +
               "\nBody:\n" +
               body +
               "\nDuration:\n" +
               duration +
               "\nRecording URL:\n" +
               recordingURL;
    }
}
