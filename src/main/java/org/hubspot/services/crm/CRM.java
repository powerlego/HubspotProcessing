package org.hubspot.services.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMProperties;
import org.hubspot.services.HttpService;

/**
 * @author Nicholas Curl
 */
public class CRM {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final HttpService httpService;
    public CRM(HttpService httpService){
        this.httpService = httpService;
    }

    public HSContactService contact() {
        return new HSContactService(httpService);
    }

    public CRMProperties contactProperties(){
        return new CRMProperties(httpService, CRMProperties.Types.CONTACTS);
    }
}
