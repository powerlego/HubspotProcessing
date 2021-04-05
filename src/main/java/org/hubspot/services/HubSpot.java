package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.HttpService;

/**
 * @author Nicholas Curl
 */
public class HubSpot {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final HttpService httpService;


    public HubSpot(String apiKey) {
        String apiBase = "https://api.hubapi.com";
        httpService = new HttpService(apiKey, apiBase);
    }


    public CRM crm() {
        return new CRM(httpService);
    }
}
