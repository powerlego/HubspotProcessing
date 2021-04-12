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
import java.util.ArrayList;
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
        long lastExecuted = Utils.readLastExecution();
        if (lastExecuted == -1) {
            lastExecuted = Utils.writeLastExecution();
        }
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        try {
            Files.createDirectories(Paths.get("./cache"));
        }
        catch (IOException e) {
            logger.fatal("Unable to create cache folder {}", Paths.get("./cache"), e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ArrayList<Contact> contacts = !ContactService.cacheExists()
                                      ? hubspot.crm()
                                               .getAllContacts("contactinformation", true)
                                      : hubspot.crm().readContactJsons();
        ConcurrentHashMap<Long, Company> companies = !CompanyService.cacheExists()
                                                     ? hubspot.crm()
                                                              .getAllCompanies("companyinformation",
                                                                               false
                                                              )
                                                     : hubspot.crm().readCompanyJsons();
        ConcurrentHashMap<Long, EngagementData> engagements = !EngagementsProcessor.cacheExists()
                                                              ? null
                                                              : hubspot.crm().readEngagementJsons();
        ArrayList<Contact> filteredContacts = hubspot.crm().filterContacts(contacts);
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        if (engagements == null) {
            try (ProgressBar pb = Utils.createProgressBar("Processing Filtered Contacts", filteredContacts.size())) {
                forkJoinPool.submit(() -> filteredContacts.parallelStream().forEach(contact -> {
                    EngagementData engagementData = hubspot.crm().getContactEngagements(contact);
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
        }
        else {
            try (ProgressBar pb = Utils.createProgressBar("Processing Filtered Contacts", filteredContacts.size())) {
                forkJoinPool.submit(() -> filteredContacts.parallelStream().forEach(contact -> {
                    EngagementData engagementData = !engagements.containsKey(contact.getId())
                                                    ? hubspot.crm()
                                                             .getContactEngagements(contact)
                                                    : engagements.get(contact.getId());
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
        }
    }
}
