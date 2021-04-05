package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

/**
 * @author Nicholas Curl
 */
public class Engagement extends HubSpotObject {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public Engagement(long id) {
        super(id);
    }
}
