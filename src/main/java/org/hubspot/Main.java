package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.Utils;

import java.nio.file.Paths;

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
        //ConcurrentHashMap<Long, Contact> contacts = hubspot.crm().getAllContacts("contactinformation", true);
        //ConcurrentHashMap<Long, Company> companies = hubspot.crm().getAllCompanies("companyinformation", false);
        //JSONObject jsonObject = hubspot.crm().getCompanyById(5755946320L);
        //JsonIO.write(jsonObject);
        //List<JSONObject> jsonObjects = JsonIO.read();
        Utils.deleteDirectory(Paths.get("./contacts"));
        //hubspot.crm().writeContactJson("contactinformation", true);
        //hubspot.crm().writeCompanyJson("companyinformation", false);
        //ConcurrentHashMap<Long, Contact> contacts = hubspot.crm().readContactJsons();
        //ConcurrentHashMap<Long, Company> companies = hubspot.crm().readCompanyJsons();
        /*ConcurrentHashMap<Long, Contact> filterContacts = hubspot.crm().filterContacts(contacts);
        try (ProgressBar pb = Utils.createProgressBar("Processing Filtered Contacts", filterContacts.size())) {
            filterContacts.forEachValue(1, contact -> {
                EngagementsProcessor.EngagementData engagementData = hubspot.crm().getContactEngagements(contact);
                contact.setEngagementIds(engagementData.getEngagementIds());
                contact.setEngagements(engagementData.getEngagements());
                String companyProperty = contact.getProperty("company").toString();
                if (companyProperty == null || companyProperty.equalsIgnoreCase("null")) {
                    long associatedCompanyId = contact.getAssociatedCompany();
                    if (associatedCompanyId != 0) {
                        Company company = companies.get(associatedCompanyId);
                        contact.setProperty("company", company.getName());
                    }
                }
                contact.setData(contact.toJson());
                ContactWriter.write(contact);
                pb.step();
                Utils.sleep(1L);
            });
        }*/
    }
}
