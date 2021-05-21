package org.hubspot.utils.exceptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.ErrorCodes;

/**
 * @author Nicholas Curl
 */
public class HubSpotException extends Exception {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(HubSpotException.class);
    private static final long   serialVersionUID = -5998427833607746958L;
    private final        int    code;
    private              String policyName;
    private              String rawMessage;

    public HubSpotException(String message) {
        this(message, null, -1, null);
    }

    public HubSpotException(String message, String policyName, int code, Throwable cause) {
        super(message, cause);
        this.policyName = policyName;
        this.code = code;
    }

    public HubSpotException(String message, int code) {
        this(message, null, code, null);
    }

    public HubSpotException(String message, int code, Throwable cause) {
        this(message, null, code, cause);
    }

    public HubSpotException() {
        super();
        this.code = ErrorCodes.HUBSPOT_EXCEPTION.getErrorCode();
    }

    public HubSpotException(String message, String policyName, int code) {
        this(message, policyName, code, null);
    }

    public HubSpotException(String message, String policyName) {
        this(message, policyName, ErrorCodes.HUBSPOT_EXCEPTION.getErrorCode(), null);
    }

    public HubSpotException(Throwable cause, int code) {
        super(cause);
        this.code = code;
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
