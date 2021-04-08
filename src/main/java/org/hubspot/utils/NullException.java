package org.hubspot.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicholas Curl
 */
public class NullException extends Exception {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public NullException(String message) {
        super(message);
    }

    public NullException() {
        super();
    }
}
