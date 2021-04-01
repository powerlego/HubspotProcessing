package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.HSContact;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.HubSpotException;
import org.hubspot.writers.EngagementsWriter;

import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Main {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws HubSpotException {
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        List<HSContact> contacts = hubspot.crm().contact().getAllContacts();
        for(HSContact contact : contacts){
            EngagementsWriter.write(contact.getId(), contact.getEngagements());
        }
    }
}
