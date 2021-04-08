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
    private String policyName;
    private String rawMessage;

    public HubSpotException(String message) {
        this(message, null, -1, null);
    }


    public HubSpotException(String message, String policyName, int code, Throwable cause) {
        super(message, cause);
        this.policyName = policyName;
        this.code = code;
    }

    public HubSpotException(String message, Throwable cause) {
        this(message, null, -1, cause);
    }

    public HubSpotException(String message, int code) {
        this(message, null, code, null);
    }

    public HubSpotException(String message, int code, Throwable cause) {
        this(message, null, code, cause);
    }

    public HubSpotException() {
        super();
    }

    public HubSpotException(Throwable cause) {
        super(cause);
    }

    public HubSpotException(String message, String policyName, Throwable cause) {
        this(message, policyName, 0, cause);
    }

    public HubSpotException(String message, String policyName, int code) {
        this(message, policyName, code, null);
    }

    public HubSpotException(String message, String policyName) {
        this(message, policyName, -1, null);
    }

    public int getCode() {
        return code;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

}
