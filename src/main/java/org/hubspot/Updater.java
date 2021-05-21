package org.hubspot;

import com.google.common.collect.Iterables;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.hubspot.io.ContactWriter;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.HubSpot;
import org.hubspot.services.crm.CompanyService;
import org.hubspot.services.crm.ContactService;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.CustomThreadPoolExecutor;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class Updater {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger(Updater.class);
    private static final long   DELAY              = 25;
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    LIMIT              = 1;
    private static final long   WARMUP             = 10;
    private static final int    MAX_SIZE           = 75;
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static void main(String[] args) {
        CPUMonitor.startMonitoring();
        try {
            Files.createDirectories(Paths.get("./cache"));
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to create cache folder {}", Paths.get("./cache"), e);
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
        long lastFinished = FileUtils.readLastFinished();
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        HashMap<Long, Contact> contacts;
        HashMap<Long, Contact> updatedContacts;
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
        HashMap<Long, Company> companies;
        if (!CompanyService.cacheExists()) {
            companies = hubspot.crm().getAllCompanies("companyinformation", false);
        }
        else {
            companies = hubspot.crm().readCompanyJsons();
            Utils.sleep(DELAY);
            HashMap<Long, Company> updatedCompanies = hubspot.crm()
                                                             .getUpdatedCompanies("companyinformation",
                                                                                  false,
                                                                                  lastExecuted,
                                                                                  lastFinished
                                                             );
            companies.putAll(updatedCompanies);
        }
        int capacity = (int) Math.ceil(Math.ceil((double) contacts.size() / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
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
        Utils.addExecutor(threadPoolExecutor);
        Iterable<List<Long>> partitions = Iterables.partition(contacts.keySet(), LIMIT);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("MainThreadPoolUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "Main", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        ProgressBar progressBar = Utils.createProgressBar("Processing Contacts", contacts.size());
        Utils.sleep(WARMUP);
        ConcurrentHashMap<Long, Contact> concurrentContacts = new ConcurrentHashMap<>(contacts);
        List<Long> correctionIds = Collections.synchronizedList(new ArrayList<>());
        for (List<Long> partition : partitions) {
            threadPoolExecutor.submit(() -> {
                for (Long contactId : partition) {
                    Contact contact = concurrentContacts.get(contactId);
                    String companyProperty = contact.getProperty("company").toString();
                    if (companyProperty == null || companyProperty.equalsIgnoreCase("null")) {
                        long associatedCompanyId = contact.getAssociatedCompany();
                        if (associatedCompanyId != 0) {
                            Company company = companies.get(associatedCompanyId);
                            contact.setProperty("company", company.getName());
                        }
                    }
                    contact.setData(contact.toJson());
                    if (!Strings.isBlank(contact.getFirstName())) {
                        String[] split = contact.getFirstName().split(" ");
                        if (split.length > 2) {
                            correctionIds.add(contactId);
                        }
                    }
                    ContactWriter.write(contact);
                    progressBar.step();
                }

                return null;
            });
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("long_names", correctionIds);
        Path correctionFile = Paths.get("./contacts/corrections.json");
        try {
            FileUtils.writeFile(correctionFile, jsonObject.toString(4));
        }
        catch (IOException e) {
            logger.fatal("Unable to write file {}", correctionFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        progressBar.close();
        CPUMonitor.stopMonitoring();
    }

}
