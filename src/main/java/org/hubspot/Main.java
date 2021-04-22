package org.hubspot;

import com.google.common.collect.Iterables;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.io.ContactWriter;
import org.hubspot.io.EngagementsWriter;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.HubSpot;
import org.hubspot.services.crm.CompanyService;
import org.hubspot.services.crm.ContactService;
import org.hubspot.services.crm.EngagementsProcessor;
import org.hubspot.services.crm.EngagementsProcessor.EngagementData;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.FileUtils;
import org.hubspot.utils.Utils;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.CustomThreadPoolExecutor;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class Main {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    private static final long   DELAY              = 25;
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    LIMIT              = 1;
    private static final long   WARMUP             = 10;
    private static final int    MAX_SIZE           = 75;
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static void main(final String[] args) {
        CPUMonitor.startMonitoring();
        try {
            Files.createDirectories(Paths.get("./cache"));
        }
        catch (final IOException e) {
            logger.fatal("Unable to create cache folder {}", Paths.get("./cache"), e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        long lastExecuted = FileUtils.readLastExecution();
        if (lastExecuted == -1) {
            lastExecuted = FileUtils.writeLastExecution();
        }
        else {
            if (args != null && args.length > 0 && !args[0].equalsIgnoreCase("debug")) {
                FileUtils.writeLastExecution();
            }
        }
        final long lastFinished = FileUtils.readLastFinished();
        final HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        final HashMap<Long, Contact> contacts;
        final HashMap<Long, Contact> updatedContacts;
        if (!ContactService.cacheExists()) {
            contacts = hubspot.crm().getAllContacts("contactinformation", true);
            updatedContacts = new HashMap<>();
        }
        else {
            contacts = hubspot.crm().readContactJsons();
            Utils.sleep(DELAY);
            updatedContacts = hubspot.crm().getUpdatedContacts("contactinformation", true, lastExecuted, lastFinished);
            contacts.putAll(updatedContacts);
        }
        Utils.sleep(DELAY);
        final HashMap<Long, Company> companies;
        if (!CompanyService.cacheExists()) {
            companies = hubspot.crm().getAllCompanies("companyinformation", false);
        }
        else {
            companies = hubspot.crm().readCompanyJsons();
            Utils.sleep(DELAY);
            final HashMap<Long, Company> updatedCompanies = hubspot.crm()
                                                                   .getUpdatedCompanies("companyinformation",
                                                                                        false,
                                                                                        lastExecuted,
                                                                                        lastFinished
                                                                   );
            companies.putAll(updatedCompanies);
        }
        final HashMap<Long, EngagementData> engagements;
        if (!EngagementsProcessor.cacheExists()) {
            engagements = null;
        }
        else {
            engagements = hubspot.crm().readEngagementJsons();
        }
        //final HashMap<Long, Contact> filteredContacts = hubspot.crm().filterContacts(contacts);
        final int capacity = (int) Math.ceil(Math.ceil((double) contacts.size() / (double) LIMIT) *
                                             Math.pow(MAX_SIZE, -0.6));
        final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                         STARTING_POOL_SIZE,
                                                                                         0L,
                                                                                         TimeUnit.MILLISECONDS,
                                                                                         new LinkedBlockingQueue<>(Math.max(
                                                                                                 capacity,
                                                                                                 Runtime.getRuntime()
                                                                                                        .availableProcessors()
                                                                                         )),
                                                                                         new CustomThreadFactory(
                                                                                                 "MainThreadPool"),
                                                                                         new StoringRejectedExecutionHandler()
        );
        final Iterable<List<Long>> partitions = Iterables.partition(contacts.keySet(), LIMIT);
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("MainThreadPoolUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "Main", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        final ProgressBar progressBar = Utils.createProgressBar("Processing Contacts", contacts.size());
        Utils.sleep(WARMUP);
        final ConcurrentHashMap<Long, Contact> concurrentContacts = new ConcurrentHashMap<>(contacts);
        if (engagements == null) {
            for (final List<Long> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final long contactId : partition) {
                        final Contact contact = concurrentContacts.get(contactId);
                        final EngagementData engagementData = hubspot.crm().getContactEngagements(contact);
                        processContact(hubspot, companies, contact, engagementData, progressBar);
                    }
                    return null;
                });
            }
        }
        else {
            for (final List<Long> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final Long contactId : partition) {
                        final Contact contact = concurrentContacts.get(contactId);
                        final EngagementData engagementData;
                        if (!engagements.containsKey(contactId)) {
                            engagementData = hubspot.crm().getContactEngagements(contact);
                        }
                        else if (updatedContacts.containsKey(contactId)) {
                            engagementData = engagements.get(contactId);
                            final EngagementData updatedEngagements = hubspot.crm()
                                                                             .getUpdatedEngagements(contact,
                                                                                                    lastFinished
                                                                             );
                            engagementData.getEngagementIds().addAll(updatedEngagements.getEngagementIds());
                            engagementData.getEngagements().addAll(updatedEngagements.getEngagements());
                        }
                        else {
                            engagementData = engagements.get(contactId);
                        }
                        processContact(hubspot, companies, contact, engagementData, progressBar);
                    }
                });
            }
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        progressBar.close();
        FileUtils.writeLastFinished();
        CPUMonitor.stopMonitoring();
    }

    private static void processContact(final HubSpot hubspot,
                                       final HashMap<Long, Company> companies,
                                       final Contact contact,
                                       final EngagementData engagementData,
                                       final ProgressBar progressBar
    ) {
        if (!engagementData.getEngagements().isEmpty()) {
            hubspot.cms().getAllNoteAttachments(contact.getId(), engagementData.getEngagements());
        }
        contact.setEngagementIds(engagementData.getEngagementIds());
        contact.setEngagements(engagementData.getEngagements());
        final String companyProperty = contact.getProperty("company").toString();
        if (companyProperty == null || companyProperty.equalsIgnoreCase("null")) {
            final long associatedCompanyId = contact.getAssociatedCompany();
            if (associatedCompanyId != 0) {
                final Company company = companies.get(associatedCompanyId);
                contact.setProperty("company", company.getName());
            }
        }
        contact.setData(contact.toJson());
        ContactWriter.write(contact);
        EngagementsWriter.write(hubspot, contact.getId(), engagementData.getEngagements());
        progressBar.step();
    }
}
