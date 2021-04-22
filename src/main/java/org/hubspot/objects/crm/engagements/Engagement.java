package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

/**
 * The parent class that represents any Hubspot engagement
 *
 * @author Nicholas Curl
 */
public class Engagement extends HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long   serialVersionUID = -2283094077997225535L;

    /**
     * A constructor for an Engagement object
     *
     * @param id The engagement id
     */
    public Engagement(final long id) {
        super(id);
    }
}
