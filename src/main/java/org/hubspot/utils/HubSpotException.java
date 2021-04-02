package org.hubspot.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicholas Curl
 */
public class HubSpotException extends Exception {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private int code;
    private String rawMessage;

    public HubSpotException(String message) {
        super(message);
    }

    public HubSpotException(String message, Throwable cause) {
        super(message, cause);
    }

    public HubSpotException(String message, int code) {
        super(message);
        this.code = code;
    }

    public HubSpotException(String message, String rawMessage) {
        super(message);
        this.rawMessage = rawMessage;
    }

    public int getCode() {
        return code;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }
}
