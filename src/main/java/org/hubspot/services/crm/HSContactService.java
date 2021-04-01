package org.hubspot.services.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertiesLoader;
import org.hubspot.objects.crm.HSContact;
import org.hubspot.objects.crm.CRMProperties;
import org.hubspot.services.HttpService;
import org.hubspot.utils.CustomThreadFactory;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
import org.json.JSONArray;
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
public class HSContactService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final HttpService httpService;
    private final String propertyString;

    public HSContactService(HttpService httpService) {
        this.httpService = httpService;
        List<String> allProperties = new CRMProperties(httpService, CRMProperties.Types.CONTACTS).getPropertyNames();
        List<String> properties = PropertiesLoader.loadProperties("contacts");
        if(properties.isEmpty()){
            propertyString = Utils.propertyListToString(allProperties);
        }
        else {
            propertyString = Utils.propertyListToString(properties);
        }
    }

    public List<HSContact> getAllContacts() throws HubSpotException {
        String url = "/crm/v3/objects/contacts/";
        Map<String, Object> map = new HashMap<>();
        map.put("limit", 10);
        map.put("archived", false);
        map.put("properties", propertyString);
        String after;
        List<HSContact> contacts = Collections.synchronizedList(new LinkedList<>());
        AtomicInteger completed = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("ContactGrabber"));
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long startTime = System.nanoTime();
        Runnable progress = ()->{
            long currTime = System.nanoTime();
            long elapsed = currTime-startTime;
            long durationInMills = TimeUnit.NANOSECONDS.toMillis(elapsed);
            long millis = durationInMills % 1000;
            long second = (durationInMills / 1000) % 60;
            long minute = (durationInMills / (1000 * 60)) % 60;
            long hour = (durationInMills / (1000 * 60 * 60)) % 24;
            String duration = String.format("%02d:%02d:%02d.%d", hour, minute, second, millis);
            String formatInfo = "%s%-6s\t%s%s";
            String info = String.format(formatInfo,"Completed: ", completed.get(), "Elapsed Time: ", duration);
            logger.debug(info);
        };
        scheduledExecutorService.scheduleAtFixedRate(progress,0, 10, TimeUnit.SECONDS);
        while (true) {
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            Runnable process = ()-> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    HSContact contact = parseContactData(contactJson);
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
        } catch (InterruptedException e){
            logger.warn("Thread interrupted", e);
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e){
            logger.warn("Thread interrupted", e);
        }
        return contacts;
    }

    public HSContact parseContactData(JSONObject jsonObject) {
        HSContact HSContact = new HSContact();
        long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        HSContact.setId(id);
        JSONObject jsonProperties = jsonObject.getJSONObject("properties");
        Set<String> keys = jsonProperties.keySet();

        for (String key : keys) {
            Object jsonPropertyObject = jsonProperties.get(key);
            if (jsonPropertyObject instanceof JSONObject) {
                JSONObject jsonProperty = (JSONObject) jsonPropertyObject;
                HSContact.setProperty(key, jsonProperty.getString("value"));
            } else {
                if (jsonPropertyObject == null) {
                    HSContact.setProperty(key, null);
                } else {
                    HSContact.setProperty(key, jsonPropertyObject.toString());
                }
            }
        }
        try {
            HSContact.setEngagementIds(getEngagements(id));

        }catch (HubSpotException e){
            logger.warn("Could not get engagements for contact id "+id+"\nReason: "+e.getMessage());
            HSContact.setEngagementIds(new LinkedList<>());
        }
        return HSContact;
    }

    public HSContact getByID(long id) throws HubSpotException {
        String url = "/crm/v3/objects/contacts/" + id;
        return getContact(url);
    }

    private HSContact getContact(String url) throws HubSpotException {
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

    public List<Long> getEngagements(long id) throws HubSpotException{
        String url = "/crm-associations/v1/associations/" + id + "/HUBSPOT_DEFINED/9";
        Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", 10);
        List<Long> notes = Collections.synchronizedList(new LinkedList<>());
        long offset;
        ExecutorService executorService = Executors.newFixedThreadPool(10, new CustomThreadFactory(id+"_engagements"));
        try {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
                JSONArray jsonNotes = jsonObject.getJSONArray("results");
                Runnable runnable = ()-> {
                    for (int i = 0; i < jsonNotes.length(); i++) {
                        /*String noteUrl = "/engagements/v1/engagements/" + jsonNotes.getLong(i);
                        JSONObject jsonNote = null;
                        try {
                            jsonNote = (JSONObject) httpService.getRequest(noteUrl);
                        } catch (HubSpotException e) {
                            logger.fatal("Error occurred while getting engagements.", e);
                            System.exit(-1);
                        }
                        Object o = null;
                        if(jsonNote != null) {
                            o = EngagementsProcessor.process(jsonNote);
                        }*/
                        notes.add(jsonNotes.getLong(i));
                    }
                };
                executorService.submit(runnable);
                if (!jsonObject.getBoolean("hasMore")) {
                    break;
                }
                offset = jsonObject.getLong("offset");
                queryParam.put("offset", offset);
                Utils.sleep(500L);
            }
            executorService.shutdown();
            try{
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e){
                logger.warn("Thread interrupted", e);
            }
            return notes;
        } catch (HubSpotException e){
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }


}
