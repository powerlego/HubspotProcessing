package org.hubspot.services;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.services.cms.CMS;
import org.hubspot.services.crm.CRM;
import org.hubspot.utils.HttpService;

/**
 * @author Nicholas Curl
 */
public class HubSpot {

    /**
     * The instance of the logger
     */
    private static final Logger      logger = LogManager.getLogger();
    private final        HttpService httpService;
    private final        RateLimiter rateLimiter;


    public HubSpot(String apiKey) {
        String apiBase = "https://api.hubapi.com";
        httpService = new HttpService(apiKey, apiBase);
        this.rateLimiter = RateLimiter.create(15.0);
    }

    public CMS cms() {
        return new CMS(httpService, rateLimiter);
    }

    public CRM crm() {
        return new CRM(httpService, rateLimiter);
    }
}
