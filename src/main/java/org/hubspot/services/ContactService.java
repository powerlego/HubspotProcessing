package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.CustomThreadFactory;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.json.JSONObject;

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

    static List<Contact> getAllContacts(HttpService httpService, String propertyString) throws HubSpotException {
        String url = "/crm/v3/objects/contacts/";
        Map<String, Object> map = new HashMap<>();
        map.put("limit", 10);
        map.put("archived", false);
        map.put("properties", propertyString);
        String after;
        List<Contact> contacts = Collections.synchronizedList(new LinkedList<>());
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = () -> {
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
        };
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
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted", e);
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted", e);
        }
        return contacts;
    }

    static Contact parseContactData(JSONObject jsonObject) {
        Contact HSContact = new Contact();
        long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        HSContact.setId(id);
        JSONObject jsonProperties = jsonObject.getJSONObject("properties");
        Set<String> keys = jsonProperties.keySet();

        for (String key : keys) {
            Object jsonPropertyObject = jsonProperties.get(key);
            if (jsonPropertyObject instanceof JSONObject) {
                JSONObject jsonProperty = (JSONObject) jsonPropertyObject;
                String propertyValue = jsonProperty.getString("value");
                propertyValue = propertyValue.strip();
                propertyValue = propertyValue.replace("\"", "");
                HSContact.setProperty(key, propertyValue);
            } else {
                if (jsonPropertyObject == null) {
                    HSContact.setProperty(key, null);
                } else {
                    HSContact.setProperty(key, jsonPropertyObject.toString());
                }
            }
        }
        return HSContact;
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


}
