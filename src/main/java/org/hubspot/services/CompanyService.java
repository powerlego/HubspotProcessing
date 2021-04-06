package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.json.JSONObject;

/**
 * @author Nicholas Curl
 */
public class CompanyService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    static JSONObject getByID(HttpService service, String propertyString, long id) throws HubSpotException {
        String url = "/crm/v3/objects/companies/" + id;
        return getCompany(service, propertyString, url);
    }

    static JSONObject getCompany(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return (JSONObject) httpService.getRequest(url, propertyString);
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }
}
