package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMProperties.PropertyData;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.CustomThreadFactory;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

    static List<Contact> filterContacts(List<Contact> contacts) {
        List<Contact> filteredContacts = new LinkedList<>();
        List<Contact> rest = new LinkedList<>();
        for (Contact contact : contacts) {
            String lifeCycleStage = contact.getLifeCycleStage();
            String leadStatus = contact.getLeadStatus();
            String lifecycleOtherReason = contact.getProperty("hr_hiring_applicant").toString();
            boolean b = leadStatus == null || (!leadStatus.toLowerCase().contains("closed") && !leadStatus.equalsIgnoreCase("Recruit") && !leadStatus.toLowerCase().contains("no contact") && !leadStatus.toLowerCase().contains("unqualified"));
            boolean c = (lifecycleOtherReason == null || lifecycleOtherReason.equalsIgnoreCase("null")) || !lifecycleOtherReason.toLowerCase().contains("closed");
            if (lifeCycleStage == null) {
                if (b) {
                    if (c) {
                        filteredContacts.add(contact);
                    } else {
                        rest.add(contact);
                    }
                }
            } else if (!lifeCycleStage.equalsIgnoreCase("subscriber")) {
                if (b) {
                    if (c) {
                        filteredContacts.add(contact);
                    } else {
                        rest.add(contact);
                    }
                }
            }
        }
        return filteredContacts;
    }

    static List<Contact> getAllContacts(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        List<Contact> contacts = Collections.synchronizedList(new LinkedList<>());
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        String url = "/crm/v3/objects/contacts/";
        long after;
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        long startTime = System.nanoTime();
        Runnable progress = () -> getCompleted(completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        while (true) {
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            Runnable process = () -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    Contact contact = parseContactData(contactJson);

                    contacts.add(contact);
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
        shutdownExecutors(executorService, completed, scheduledExecutorService, startTime);
        return contacts;
    }

    private static void getCompleted(AtomicInteger completed, long startTime) {
        long currTime = System.nanoTime();
        long elapsed = currTime - startTime;
        long durationInMills = TimeUnit.NANOSECONDS.toMillis(elapsed);
        long millis = durationInMills % 1000;
        long second = (durationInMills / 1000) % 60;
        long minute = (durationInMills / (1000 * 60)) % 60;
        long hour = (durationInMills / (1000 * 60 * 60)) % 24;
        String duration = String.format("%02d:%02d:%02d.%d", hour, minute, second, millis);
        String formatInfo = "%s%-6s\t%s%s";
        String info = String.format(formatInfo, "Completed: ", completed.get(), "Elapsed Time: ", duration);
        logger.debug(info);
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
        contact.setData();
        return contact;
    }

    private static void shutdownExecutors(ExecutorService executorService, AtomicInteger completed, ScheduledExecutorService scheduledExecutorService, long startTime) {
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        } catch (InterruptedException e) {
            logger.warn("Thread interrupted", e);
        }
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
            getCompleted(completed, startTime);
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted", e);
        }
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

    static List<Contact> readContactJsons() {
        List<Contact> contacts = new LinkedList<>();
        Path jsonFolder = Paths.get("./cache/contacts/");
        File[] files = jsonFolder.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                String jsonString = Utils.readFile(file);
                JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                contacts.add(parseContactData(jsonObject));
            }
            return contacts;
        } else {
            return new LinkedList<>();
        }
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
        Runnable progress = () -> getCompleted(completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        Path jsonFolder = Paths.get("./cache/contacts/");
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
        shutdownExecutors(executorService, completed, scheduledExecutorService, startTime);
    }

}
