package org.hubspot.services;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.Utils;
import org.hubspot.utils.concurrent.AutoShutdown;
import org.hubspot.utils.concurrent.CacheThreadPoolExecutor;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.CustomThreadPoolExecutor;
import org.hubspot.utils.exceptions.HubSpotException;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class ContactService {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    private static final int    LIMIT              = 10;
    private static final Path   cacheFolder        = Paths.get("./cache/contacts/");
    private static final String url                = "/crm/v3/objects/contacts/";
    private static final long   WARMUP             = 10;
    private static final int    STARTING_POOL_SIZE = 4;
    private static final long   LOOP_DELAY         = 2;
    private static final long   UPDATE_INTERVAL    = 1000;
    private static final String debugMessageFormat = "Method %-20s\tProcess Load: %f";

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static HashMap<Long, Contact> filterContacts(HashMap<Long, Contact> contacts) throws HubSpotException {
        ConcurrentHashMap<Long, Contact> concurrentContacts = new ConcurrentHashMap<>(contacts);
        ConcurrentHashMap<Long, Contact> filteredContacts = new ConcurrentHashMap<>();
        Iterable<List<Long>> partitions = Iterables.partition(contacts.keySet(), LIMIT);
        ThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                             STARTING_POOL_SIZE,
                                                                             0L,
                                                                             TimeUnit.MILLISECONDS,
                                                                             new LinkedBlockingQueue<>(),
                                                                             new CustomThreadFactory("ContactFilter")
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactFilterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "filterContacts", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 &&
                     threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime().availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try (ProgressBar pb = Utils.createProgressBar("Filtering", contacts.size())) {
            Utils.sleep(WARMUP);
            for (List<Long> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (long contactId : partition) {
                        Contact contact = concurrentContacts.get(contactId);
                        String lifeCycleStage = contact.getLifeCycleStage();
                        String leadStatus = contact.getLeadStatus();
                        String lifecycleOR = contact.getProperty("hr_hiring_applicant").toString();
                        boolean b = ((leadStatus == null || leadStatus.equalsIgnoreCase("null")) ||
                                     (!leadStatus.toLowerCase().contains("closed") &&
                                      !leadStatus.equalsIgnoreCase("Recruit") &&
                                      !leadStatus.toLowerCase().contains("no contact") &&
                                      !leadStatus.toLowerCase().contains("unqualified")
                                     )
                        );
                        boolean c = ((lifecycleOR == null || lifecycleOR.equalsIgnoreCase("null")) ||
                                     !lifecycleOR.toLowerCase().contains("closed")
                        );
                        if ((lifeCycleStage == null || !lifeCycleStage.equalsIgnoreCase("subscriber")) && b && c) {
                            filteredContacts.put(contactId, contact);
                        }
                        pb.step();
                    }
                    return null;
                });
                Utils.sleep(LOOP_DELAY);
            }
            /*while (futures.size() > 0) {
                try {
                    Future<Void> f = completionService.take();
                    futures.remove(f);
                    f.get();
                }
                catch (ExecutionException | InterruptedException e) {
                    logger.fatal("Unable to filter contacts", e);
                    System.exit(ErrorCodes.FILTER_EXCEPTION.getErrorCode());
                }
            }*/
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        scheduledExecutorService.shutdown();
        return new HashMap<>(filteredContacts);
    }

    static HashMap<Long, Contact> getAllContacts(HttpService httpService,
                                                 PropertyData propertyData,
                                                 final RateLimiter rateLimiter
    ) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                            STARTING_POOL_SIZE,
                                                                            0L,
                                                                            TimeUnit.MILLISECONDS,
                                                                            new LinkedBlockingQueue<>(),
                                                                            new CustomThreadFactory("ContactGrabber"),
                                                                            cacheFolder
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactGrabberUpdater"));
        try (ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Contacts", count)) {
            Utils.sleep(WARMUP);
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "getAllContacts", load);
                logger.debug(debugMessage);
                int comparison = Double.compare(load, 50.0);
                if (comparison > 0) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                    threadPoolExecutor.setCorePoolSize(numThreads);
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
                else if (comparison < 0 &&
                         threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime().availableProcessors()) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
            }, 0, 1, TimeUnit.SECONDS);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                threadPoolExecutor.submit(process(contacts, pb, jsonObject));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor, cacheFolder);
            scheduledExecutorService.shutdown();
        }
        return new HashMap<>(contacts);
    }

    @NotNull
    private static Callable<Void> process(ConcurrentHashMap<Long, Contact> contacts,
                                          ProgressBar pb,
                                          JSONObject jsonObject
    ) {
        return () -> {
            for (Object o : jsonObject.getJSONArray("results")) {
                JSONObject contactJson = (JSONObject) o;
                contactJson = Utils.formatJson(contactJson);
                Contact contact = parseContactData(contactJson);
                Utils.writeJsonCache(cacheFolder, contactJson);
                contacts.put(contact.getId(), contact);
                pb.step();
            }
            return null;
        };
    }

    static Contact parseContactData(JSONObject jsonObject) {
        long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        Contact contact = new Contact(id);
        JSONObject jsonProperties = jsonObject.getJSONObject("properties");
        Set<String> keys = jsonProperties.keySet();
        for (String key : keys) {
            Object jsonPropertyObject = jsonProperties.get(key);
            if (jsonPropertyObject instanceof JSONObject) {
                JSONObject jsonProperty = (JSONObject) jsonPropertyObject;
                Object propertyValue = jsonProperty.get("value");
                if (propertyValue instanceof String) {
                    String string = ((String) propertyValue).strip()
                                                            .replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                    contact.setProperty(key, string);
                }
                else {
                    contact.setProperty(key, propertyValue);
                }
            }
            else if (jsonPropertyObject == null) {
                contact.setProperty(key, null);
            }
            else if (jsonPropertyObject instanceof String) {
                String propertyValue = ((String) jsonPropertyObject).strip();
                if (key.equalsIgnoreCase("firstname") || key.equalsIgnoreCase("lastname")) {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\:_.\\-@;,]", "");
                }
                else {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                }
                contact.setProperty(key, propertyValue);
            }
            else {
                contact.setProperty(key, jsonPropertyObject);
            }
        }
        contact.setData(contact.toJson());
        return contact;
    }

    static Contact getByID(HttpService service, String propertyString, long id, final RateLimiter rateLimiter)
    throws HubSpotException {
        String urlString = url + id;
        rateLimiter.acquire(1);
        return getContact(service, propertyString, urlString);
    }

    static Contact getContact(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return parseContactData((JSONObject) httpService.getRequest(url, propertyString));
        }
        catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            }
            else {
                throw e;
            }
        }
    }

    public static Path getCacheFolder() {
        return cacheFolder;
    }

    static HashMap<Long, Contact> getUpdatedContacts(HttpService httpService,
                                                     PropertyData propertyData,
                                                     long lastExecution,
                                                     long lastFinished
    ) throws HubSpotException {
        String url = "/crm/v3/objects/contacts/search";
        final RateLimiter rateLimiter = RateLimiter.create(3.0);
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        JSONObject body = Utils.getUpdateBody(CRMObjectType.CONTACTS, propertyData, lastExecution, LIMIT);
        long after;
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                                                                       STARTING_POOL_SIZE,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new CustomThreadFactory("ContactUpdater")
        );
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2,
                                                                                             new CustomThreadFactory(
                                                                                                     "ContactUpdaterUpdater")
        );
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        List<Future<Void>> futures = new ArrayList<>();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getUpdatedContacts", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 &&
                     threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime().availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        long count = Utils.getUpdateCount(httpService, rateLimiter, CRMObjectType.CONTACTS, lastExecution);
        try (ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Contacts", count)) {
            Utils.sleep(WARMUP);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
                futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor,
                                                                        process(contacts, pb, jsonObject)
                )));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                body.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.waitForCompletion(completionService, futures);
            Utils.shutdownUpdateExecutors(logger, threadPoolExecutor, cacheFolder, lastFinished);
            scheduledExecutorService.shutdown();
        }
        return new HashMap<>(contacts);
    }

    static HashMap<Long, Contact> readContactJsons() throws HubSpotException {
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        File[] files = cacheFolder.toFile().listFiles();
        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                                                                           STARTING_POOL_SIZE,
                                                                           0L,
                                                                           TimeUnit.MILLISECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new CustomThreadFactory("ContactReader")
            );
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2,
                                                                                                 new CustomThreadFactory(
                                                                                                         "ContactReaderUpdater")
            );
            ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
            List<Future<Void>> futures = new ArrayList<>();
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "readContactJsons", load);
                logger.debug(debugMessage);
                int comparison = Double.compare(load, 50.0);
                if (comparison > 0) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                    threadPoolExecutor.setCorePoolSize(numThreads);
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
                else if (comparison < 0 &&
                         threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime().availableProcessors()) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                    threadPoolExecutor.setCorePoolSize(numThreads);
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            try (ProgressBar pb = Utils.createProgressBar("Reading Contacts", fileList.size())) {
                Utils.sleep(WARMUP);
                for (List<File> partition : partitions) {
                    futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor, () -> {
                        for (File file : partition) {
                            String jsonString = Utils.readJsonString(logger, file);
                            JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                            Contact contact = parseContactData(jsonObject);
                            contacts.put(contact.getId(), contact);
                            pb.step();
                        }
                        return null;
                    })));
                    Utils.sleep(LOOP_DELAY);
                }
                while (futures.size() > 0) {
                    try {
                        Future<Void> f = completionService.take();
                        futures.remove(f);
                        f.get();
                    }
                    catch (ExecutionException | InterruptedException e) {
                        logger.fatal("Unable to read contact cache", e);
                        System.exit(ErrorCodes.IO_READ.getErrorCode());
                    }
                }
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            scheduledExecutorService.shutdown();
        }
        return new HashMap<>(contacts);
    }

    static void writeContactJson(HttpService httpService, PropertyData propertyData, final RateLimiter rateLimiter)
    throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,
                                                                       STARTING_POOL_SIZE,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new CustomThreadFactory("ContactWriter")
        );
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2,
                                                                                             new CustomThreadFactory(
                                                                                                     "ContactWriterUpdater")
        );
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        List<Future<Void>> futures = new ArrayList<>();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "writeContactJson", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 &&
                     threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime().availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Writing Contacts", count)) {
            Utils.sleep(WARMUP);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor, () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject contactJson = (JSONObject) o;
                        contactJson = Utils.formatJson(contactJson);
                        Utils.writeJsonCache(cacheFolder, contactJson);
                        pb.step();
                    }
                    return null;
                })));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.waitForCompletion(completionService, futures);
            Utils.shutdownExecutors(logger, threadPoolExecutor, cacheFolder);
            scheduledExecutorService.shutdown();
        }
    }
}
