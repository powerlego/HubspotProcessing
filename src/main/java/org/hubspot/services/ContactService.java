package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Nicholas Curl
 */
class ContactService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final int LIMIT = 10;

    static Map<Long, Contact> filterContacts(Map<Long, Contact> contacts) {
        HashMap<Long, Contact> filteredContacts = new HashMap<>();
        for (long contactId : contacts.keySet()) {
            Contact contact = contacts.get(contactId);
            String lifeCycleStage = contact.getLifeCycleStage();
            String leadStatus = contact.getLeadStatus();
            String lifecycleOtherReason = contact.getProperty("hr_hiring_applicant").toString();
            boolean b = leadStatus == null || (!leadStatus.toLowerCase().contains("closed") && !leadStatus.equalsIgnoreCase("Recruit") && !leadStatus.toLowerCase().contains("no contact") && !leadStatus.toLowerCase().contains("unqualified"));
            boolean c = (lifecycleOtherReason == null || lifecycleOtherReason.equalsIgnoreCase("null")) || !lifecycleOtherReason.toLowerCase().contains("closed");
            if ((lifeCycleStage == null || !lifeCycleStage.equalsIgnoreCase("subscriber")) && b && c) {
                filteredContacts.put(contactId, contact);
            }
        }
        return filteredContacts;
    }

    static Map<Long, Contact> getAllContacts(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Contact> contacts = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = () -> Utils.getCompleted(logger, completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        while (true) {
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            Runnable process = () -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    Contact contact = parseContactData(contactJson);
                    contacts.put(contact.getId(), contact);
                    completed.getAndIncrement();
                }
            };
            executorService.submit(process);
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, executorService, completed, scheduledExecutorService, startTime);
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

    static Map<Long, Contact> readContactJsons() {
        HashMap<Long, Contact> contacts = new HashMap<>();
        Path jsonFolder = Paths.get("./cache/contacts/");
        File[] files = jsonFolder.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                String jsonString = Utils.readFile(file);
                JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                Contact contact = parseContactData(jsonObject);
                contacts.put(contact.getId(), contact);
            }
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
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = () -> Utils.getCompleted(logger, completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        Path jsonFolder = Paths.get("./cache/contacts/");
        try {
            Files.createDirectories(jsonFolder);
        } catch (IOException e) {
            logger.fatal("Unable to create folder", e);
            System.exit(-1);
        }
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
                    completed.getAndIncrement();
                }
            };
            executorService.submit(process);
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, executorService, completed, scheduledExecutorService, startTime);
    }

}
