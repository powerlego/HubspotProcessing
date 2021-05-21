package org.hubspot.utils.exceptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicholas Curl
 */
public class NullException extends Exception {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(NullException.class);
    private static final long   serialVersionUID = -7761586587464190270L;

    public NullException(String message) {
        super(message);
    }

    public NullException() {
        super();
    }
}
