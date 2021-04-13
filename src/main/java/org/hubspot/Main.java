package org.hubspot;

import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.io.ContactWriter;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.CompanyService;
import org.hubspot.services.ContactService;
import org.hubspot.services.EngagementsProcessor;
import org.hubspot.services.EngagementsProcessor.EngagementData;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class Main {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        try {
            Files.createDirectories(Paths.get("./cache"));
        }
        catch (IOException e) {
            logger.fatal("Unable to create cache folder {}", Paths.get("./cache"), e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        long lastExecuted = Utils.readLastExecution();
        if (lastExecuted == -1) {
            lastExecuted = Utils.writeLastExecution();
        }
        else {
            if (args != null && args.length > 0) {
                if (!args[0].equalsIgnoreCase("debug")) {
                    Utils.writeLastExecution();
                }
            }

        }
        long lastFinished = Utils.readLastFinished();
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        ConcurrentHashMap<Long, Contact> contacts;
        ConcurrentHashMap<Long, Contact> updatedContacts;
        if (!ContactService.cacheExists()) {
            contacts = hubspot.crm().getAllContacts("contactinformation", true);
            updatedContacts = new ConcurrentHashMap<>();
        }
        else {
            contacts = hubspot.crm().readContactJsons();
            updatedContacts = hubspot.crm().getUpdatedContacts("contactinformation", true, lastExecuted, lastFinished);
            contacts.putAll(updatedContacts);
        }
        ConcurrentHashMap<Long, Company> companies;
        if (!CompanyService.cacheExists()) {
            companies = hubspot.crm().getAllCompanies("companyinformation", false);
        }
        else {
            companies = hubspot.crm().readCompanyJsons();
            ConcurrentHashMap<Long, Company> updatedCompanies = hubspot.crm()
                                                                       .getUpdatedCompanies("companyinformation",
                                                                                            false,
                                                                                            lastExecuted,
                                                                                            lastFinished
                                                                       );
            companies.putAll(updatedCompanies);
        }
        ConcurrentHashMap<Long, EngagementData> engagements;
        if (!EngagementsProcessor.cacheExists()) {
            engagements = null;
        }
        else {
            engagements = hubspot.crm().readEngagementJsons();
        }
        ConcurrentHashMap<Long, Contact> filteredContacts = hubspot.crm().filterContacts(contacts);
        ForkJoinPool forkJoinPool = new ForkJoinPool(8);
        ProgressBar progressBar = Utils.createProgressBar("Processing Filtered Contacts", filteredContacts.size());
        if (engagements == null) {
            forkJoinPool.submit(() -> filteredContacts.forEach((contactId, contact) -> {
                EngagementData engagementData = hubspot.crm().getContactEngagements(contact);
                processContact(companies, contact, engagementData, progressBar);
            }));
        }
        else {
            forkJoinPool.submit(() -> filteredContacts.forEach((contactId, contact) -> {
                EngagementData engagementData;
                if (!engagements.containsKey(contactId)) {
                    engagementData = hubspot.crm().getContactEngagements(contact);
                }
                else if (updatedContacts.containsKey(contactId)) {
                    engagementData = engagements.get(contactId);
                    EngagementData updatedEngagements = hubspot.crm().getUpdatedEngagements(contact, lastFinished);
                    engagementData.getEngagementIds().addAll(updatedEngagements.getEngagementIds());
                    engagementData.getEngagements().addAll(updatedEngagements.getEngagements());
                }
                else {
                    engagementData = engagements.get(contactId);
                }
                processContact(companies, contact, engagementData, progressBar);
            }));
        }

        forkJoinPool.shutdown();
        try {
            if (!forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
        progressBar.close();
        Utils.writeLastFinished();
    }

    private static void processContact(ConcurrentHashMap<Long, Company> companies,
                                       Contact contact,
                                       EngagementData engagementData,
                                       ProgressBar progressBar
    ) {
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
        progressBar.step();
        Utils.sleep(1L);
    }
}
