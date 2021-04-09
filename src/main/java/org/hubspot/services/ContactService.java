package org.hubspot.services;

import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.*;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class ContactService {

    /**
     * The instance of the logger
     */
    private static final Logger logger      = LogManager.getLogger();
    private static final int    LIMIT       = 10;
    private static final Path   cacheFolder = Paths.get("./cache/contacts/");

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static ArrayList<Contact> filterContacts(ArrayList<Contact> contacts) {
        List<Contact> filteredContacts = Collections.synchronizedList(new ArrayList<>());
        ProgressBar.wrap(contacts.parallelStream(), Utils.getProgressBarBuilder("Filtering")).forEach(contact -> {
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
                filteredContacts.add(contact);
            }
            Utils.sleep(1L);
        });
        return new ArrayList<>(filteredContacts);
    }

    static ArrayList<Contact> getAllContacts(HttpService httpService,
                                             PropertyData propertyData,
                                             RateLimiter rateLimiter
    )
    throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        List<Contact> contacts = Collections.synchronizedList(new ArrayList<>());
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Contacts", count)) {
            while (true) {
                rateLimiter.acquire();
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject contactJson = (JSONObject) o;
                        contactJson = Utils.formatJson(contactJson);
                        Contact contact = parseContactData(contactJson);
                        Utils.writeJsonCache(executorService, cacheFolder, contactJson);
                        contacts.add(contact);
                        pb.step();
                    }
                };
                executorService.submit(process);
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
            }
            Utils.shutdownExecutors(logger, executorService, cacheFolder);
        }
        return new ArrayList<>(contacts);
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

    static Contact getByID(HttpService service, String propertyString, long id, RateLimiter rateLimiter)
    throws HubSpotException {
        String url = "/crm/v3/objects/contacts/" + id;
        rateLimiter.acquire();
        return getContact(service, propertyString, url);
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

    static ArrayList<Contact> readContactJsons() {
        List<Contact> contacts = Collections.synchronizedList(new ArrayList<>());
        File[] files = cacheFolder.toFile().listFiles();
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        if (files != null) {
            forkJoinPool.submit(() -> ProgressBar.wrap(Arrays.stream(files).parallel(),
                                                       Utils.getProgressBarBuilder("Reading Contacts")
            ).forEach(file -> {
                String jsonString = Utils.readFile(file);
                JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                Contact contact = parseContactData(jsonObject);
                contacts.add(contact);
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
            logger.fatal("Threads interrupted during wait.", e);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
        return new ArrayList<>(contacts);
    }

    static void writeContactJson(HttpService httpService, PropertyData propertyData, RateLimiter rateLimiter)
    throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Writing Contacts", count)) {
            while (true) {
                rateLimiter.acquire();
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject contactJson = (JSONObject) o;
                        contactJson = Utils.formatJson(contactJson);
                        Utils.writeJsonCache(executorService, cacheFolder, contactJson);
                        pb.step();
                        Utils.sleep(1L);
                    }
                };
                executorService.submit(process);
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
            }
            Utils.shutdownExecutors(logger, executorService, cacheFolder);
        }
    }
}
