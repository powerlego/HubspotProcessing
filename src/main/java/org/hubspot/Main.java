package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.io.ContactWriter;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.EngagementsProcessor;
import org.hubspot.services.HubSpot;

import java.util.Map;

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
        //JSONObject jsonObject = hubspot.crm().getCompanyById(5755946320L);
        //JsonIO.write(jsonObject);
        //List<JSONObject> jsonObjects = JsonIO.read();
        //hubspot.crm().writeContactJson("contactinformation", true);
        //hubspot.crm().writeCompanyJson("companyinformation", false);
        Map<Long, Contact> contacts = hubspot.crm().readContactJsons();
        Map<Long, Company> companies = hubspot.crm().readCompanyJsons();
        //List<Contact> contacts = hubspot.crm().getFilteredContacts("contactinformation");
        //List<Contact> contacts = hubspot.crm().getAllContacts("contactinformation");
        Map<Long, Contact> filterContacts = hubspot.crm().filterContacts(contacts);
        for (Contact contact : filterContacts.values()) {
            EngagementsProcessor.EngagementData engagementData = hubspot.crm().getContactEngagements(contact);
            contact.setEngagementIds(engagementData.getEngagementIds());
            contact.setEngagements(engagementData.getEngagements());
            String companyProperty = contact.getProperty("company").toString();
            if (companyProperty == null || companyProperty.equalsIgnoreCase("null")) {
                Company company = companies.get(contact.getAssociatedCompany());
                contact.setProperty("company", company.getName());
            }
            contact.setData(contact.toJson());
            ContactWriter.write(contact);
        }
    }
}
