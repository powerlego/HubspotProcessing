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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            if (lifeCycleStage == null) {
                if (leadStatus == null) {
                    filteredContacts.add(contact);
                } else if (!leadStatus.contains("Closed") && !leadStatus.equalsIgnoreCase("Recruit")) {
                    filteredContacts.add(contact);
                } else {
                    rest.add(contact);
                }
            } else if (!lifeCycleStage.equalsIgnoreCase("subscriber")) {
                if (leadStatus == null) {
                    filteredContacts.add(contact);
                } else if (!leadStatus.contains("Closed") && !leadStatus.equalsIgnoreCase("Recruit")) {
                    filteredContacts.add(contact);
                } else {
                    rest.add(contact);
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
        String after;
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
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getString("after");
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
                String propertyValue = jsonProperty.getString("value");
                propertyValue = propertyValue.strip();
                Pattern p = Pattern.compile("[^a-zA-Z\\d_:\\-.]");
                Matcher m = p.matcher(propertyValue);
                propertyValue = m.replaceAll("");
                contact.setProperty(key, propertyValue);
            } else {
                if (jsonPropertyObject == null) {
                    contact.setProperty(key, null);
                } else {
                    String propertyValue = jsonPropertyObject.toString();
                    propertyValue = propertyValue.strip();
                    Pattern p = Pattern.compile("[^a-zA-Z\\d_:\\-.]");
                    Matcher m = p.matcher(propertyValue);
                    propertyValue = m.replaceAll("");
                    contact.setProperty(key, propertyValue);
                }
            }
        }
        contact.setData();
        return contact;
    }

    private static void shutdownExecutors(ExecutorService executorService, AtomicInteger completed, ScheduledExecutorService scheduledExecutorService, long startTime) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
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
        List<Contact> contacts = Collections.synchronizedList(new LinkedList<>());
        Path jsonFolder = Paths.get("./cache/");
        File[] files = jsonFolder.toFile().listFiles();
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        AtomicInteger completed = new AtomicInteger();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            List<List<File>> fileListChunked = Utils.splitList(fileList, 10);
            long startTime = System.nanoTime();
            Runnable progress = () -> getCompleted(completed, startTime);
            scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
            if (fileListChunked != null) {
                for (List<File> chunkedFileList : fileListChunked) {
                    Runnable runnable = () -> {
                        for (File file : chunkedFileList) {
                            String jsonString = Utils.readFile(file);
                            JSONObject jsonObject = new JSONObject(jsonString);
                            contacts.add(parseContactData(jsonObject));
                            completed.getAndIncrement();
                        }
                    };
                    executorService.submit(runnable);
                }
            }
            shutdownExecutors(executorService, completed, scheduledExecutorService, startTime);
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
        String after;
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = () -> getCompleted(completed, startTime);
        scheduledExecutorService.scheduleAtFixedRate(progress, 0, 10, TimeUnit.SECONDS);
        Path jsonFolder = Paths.get("./cache/");
        while (true) {
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            Runnable process = () -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    long id = contactJson.has("id") ? contactJson.getLong("id") : 0;
                    Path contactFilePath = jsonFolder.resolve(id + ".txt");
                    try {
                        FileWriter fileWriter = new FileWriter(contactFilePath.toFile());
                        fileWriter.write(o.toString());
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
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getString("after");
            map.put("after", after);
        }
        shutdownExecutors(executorService, completed, scheduledExecutorService, startTime);
    }

}
