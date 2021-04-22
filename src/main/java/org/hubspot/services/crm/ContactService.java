package org.hubspot.services.crm;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.*;
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
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    MAX_SIZE           = 50;
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static HashMap<Long, Contact> filterContacts(HashMap<Long, Contact> contacts) throws HubSpotException {
        ConcurrentHashMap<Long, Contact> concurrentContacts = new ConcurrentHashMap<>(contacts);
        ConcurrentHashMap<Long, Contact> filteredContacts = new ConcurrentHashMap<>();
        Iterable<List<Long>> partitions = Iterables.partition(contacts.keySet(), LIMIT);
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
                                                                                           "ContactFilter"),
                                                                                   new StoringRejectedExecutionHandler()
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactFilterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "filterContacts", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        ProgressBar pb = Utils.createProgressBar("Filtering", contacts.size());
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
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
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
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        ThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                            STARTING_POOL_SIZE,
                                                                            0L,
                                                                            TimeUnit.MILLISECONDS,
                                                                            new LinkedBlockingQueue<>(Math.max(capacity,
                                                                                                               Runtime.getRuntime()
                                                                                                                      .availableProcessors()
                                                                            )),
                                                                            new CustomThreadFactory("ContactGrabber"),
                                                                            new StoringRejectedExecutionHandler(),
                                                                            cacheFolder
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactGrabberUpdater"));
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Contacts", count);
        Utils.sleep(WARMUP);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getAllContacts", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
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
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
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
                FileUtils.writeJsonCache(cacheFolder, contactJson);
                contacts.put(contact.getId(), contact);
                pb.step();
                Utils.sleep(1);
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
        JSONObject body = HubSpotUtils.getUpdateBody(CRMObjectType.CONTACTS, propertyData, lastExecution, LIMIT);
        long after;
        long count = HubSpotUtils.getUpdateCount(httpService, rateLimiter, CRMObjectType.CONTACTS, lastExecution);
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        UpdateThreadPoolExecutor threadPoolExecutor = new UpdateThreadPoolExecutor(1,
                                                                                   STARTING_POOL_SIZE,
                                                                                   0L,
                                                                                   TimeUnit.MILLISECONDS,
                                                                                   new LinkedBlockingQueue<>(Math.max(
                                                                                           capacity,
                                                                                           Runtime.getRuntime()
                                                                                                  .availableProcessors()
                                                                                   )),
                                                                                   new CustomThreadFactory(
                                                                                           "ContactUpdater"),
                                                                                   new StoringRejectedExecutionHandler(),
                                                                                   cacheFolder,
                                                                                   lastFinished
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactUpdaterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getUpdatedContacts", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Contacts", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
            threadPoolExecutor.submit(process(contacts, pb, jsonObject));
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            body.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
        return new HashMap<>(contacts);
    }

    static HashMap<Long, Contact> readContactJsons() throws HubSpotException {
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        File[] files = cacheFolder.toFile().listFiles();
        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            int capacity = (int) Math.ceil(Math.ceil((double) fileList.size() / (double) LIMIT) *
                                           Math.pow(MAX_SIZE, -0.6));
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
                                                                                               "ContactReader"),
                                                                                       new StoringRejectedExecutionHandler()
            );
            ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactReaderUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "readContactJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            ProgressBar pb = Utils.createProgressBar("Reading Contacts", fileList.size());
            Utils.sleep(WARMUP);
            for (List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (File file : partition) {
                        String jsonString = FileUtils.readJsonString(logger, file);
                        JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                        Contact contact = parseContactData(jsonObject);
                        contacts.put(contact.getId(), contact);
                        pb.step();
                        Utils.sleep(1);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
            pb.close();
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
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                 STARTING_POOL_SIZE,
                                                                                 0L,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 new LinkedBlockingQueue<>(Math.max(
                                                                                         capacity,
                                                                                         Runtime.getRuntime()
                                                                                                .availableProcessors()
                                                                                 )),
                                                                                 new CustomThreadFactory("ContactWriter"),
                                                                                 new StoringRejectedExecutionHandler(),
                                                                                 cacheFolder
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactWriterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "writeContactJson", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ProgressBar pb = Utils.createProgressBar("Writing Contacts", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(() -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    contactJson = Utils.formatJson(contactJson);
                    FileUtils.writeJsonCache(cacheFolder, contactJson);
                    pb.step();
                    Utils.sleep(1);
                }
                return null;
            });
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
    }
}
