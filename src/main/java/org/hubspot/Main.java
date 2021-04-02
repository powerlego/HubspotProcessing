package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.HubSpot;
import org.hubspot.writers.ContactWriter;

import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Main {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        List<Contact> contacts = hubspot.crm().getAllContacts("contactinformation");
        for (Contact contact : contacts) {
            ContactWriter.write(contact);
        }
    }
}
