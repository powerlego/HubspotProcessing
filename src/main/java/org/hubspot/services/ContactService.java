package org.hubspot.services;

import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.CustomThreadFactory;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
class ContactService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final int LIMIT = 10;

    static ConcurrentHashMap<Long, Contact> filterContacts(ConcurrentHashMap<Long, Contact> contacts) {
        ConcurrentHashMap<Long, Contact> filteredContacts = new ConcurrentHashMap<>();
        long count = contacts.keySet().size();
        try (ProgressBar pb = Utils.createProgressBar("Filtering",count)) {
            contacts.forEach(500, (aLong, contact) -> {
                String lifeCycleStage = contact.getLifeCycleStage();
                String leadStatus = contact.getLeadStatus();
                String lifecycleOtherReason = contact.getProperty("hr_hiring_applicant").toString();
                boolean b = leadStatus == null || (!leadStatus.toLowerCase().contains("closed") && !leadStatus.equalsIgnoreCase("Recruit") && !leadStatus.toLowerCase().contains("no contact") && !leadStatus.toLowerCase().contains("unqualified"));
                boolean c = (lifecycleOtherReason == null || lifecycleOtherReason.equalsIgnoreCase("null")) || !lifecycleOtherReason.toLowerCase().contains("closed");
                if ((lifeCycleStage == null || !lifeCycleStage.equalsIgnoreCase("subscriber")) && b && c) {
                    filteredContacts.put(aLong, contact);
                }
                pb.step();
                Utils.sleep(1L);
            });
        }
        return filteredContacts;
    }

    static ConcurrentHashMap<Long, Contact> getAllContacts(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        try(ProgressBar pb = Utils.createProgressBar("Grabbing Contacts", count)) {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject contactJson = (JSONObject) o;
                        Contact contact = parseContactData(contactJson);
                        contacts.put(contact.getId(), contact);
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
            Utils.shutdownExecutors(logger, executorService);
        }
        return contacts;
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
                    String string = (String) propertyValue;
                    string = string.strip();
                    string = string.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                    contact.setProperty(key, string);
                } else {
                    contact.setProperty(key, propertyValue);
                }
            } else {
                if (jsonPropertyObject == null) {
                    contact.setProperty(key, null);
                } else {
                    if (jsonPropertyObject instanceof String) {
                        String propertyValue = (String) jsonPropertyObject;
                        propertyValue = propertyValue.strip();
                        if (key.equalsIgnoreCase("firstname") || key.equalsIgnoreCase("lastname")) {
                            propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\:_.\\-@;,]", "");
                        } else {
                            propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                        }
                        contact.setProperty(key, propertyValue);
                    } else {
                        contact.setProperty(key, jsonPropertyObject);
                    }
                }
            }
        }
        contact.setData(contact.toJson());
        return contact;
    }



    static Contact getByID(HttpService service, String propertyString, long id) throws HubSpotException {
        String url = "/crm/v3/objects/contacts/" + id;
        return getContact(service, propertyString, url);
    }

    static Contact getContact(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return parseContactData((JSONObject) httpService.getRequest(url, propertyString));
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }

    static ConcurrentHashMap<Long, Contact> readContactJsons() {
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        Path jsonFolder = Paths.get("./cache/contacts/");
        File[] files = jsonFolder.toFile().listFiles();
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        if (files != null) {
            forkJoinPool.submit(()->ProgressBar.wrap(Arrays.stream(files).parallel(), Utils.getProgressBarBuilder("Reading Contacts")).forEach(file -> {
                String jsonString = Utils.readFile(file);
                JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                Contact contact = parseContactData(jsonObject);
                contacts.put(contact.getId(), contact);
                Utils.sleep(1L);
            }));
        }
        forkJoinPool.shutdown();
        try {
            if(!forkJoinPool.awaitTermination(Long.MAX_VALUE,TimeUnit.NANOSECONDS)){
                logger.warn("Termination Timeout");
            }
        } catch (InterruptedException e) {
            logger.fatal("Threads interrupted during wait.", e);
            System.exit(-1);
        }
        return contacts;
    }

    static void writeContactJson(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.CONTACTS);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        Path jsonFolder = Paths.get("./cache/contacts/");
        try {
            Files.createDirectories(jsonFolder);
        } catch (IOException e) {
            logger.fatal("Unable to create folder", e);
            System.exit(-1);
        }
        try(ProgressBar pb = Utils.createProgressBar("Writing Contacts", count)) {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject contactJson = (JSONObject) o;
                        contactJson = Utils.formatJson(contactJson);
                        long id = contactJson.has("id") ? contactJson.getLong("id") : 0;
                        Path contactFilePath = jsonFolder.resolve(id + ".json");
                        try {
                            FileWriter fileWriter = new FileWriter(contactFilePath.toFile());
                            fileWriter.write(contactJson.toString(4));
                            fileWriter.close();
                        } catch (IOException e) {
                            logger.fatal("Unable to write file for id " + id, e);
                        }
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
            Utils.shutdownExecutors(logger, executorService);
        }
    }

}
