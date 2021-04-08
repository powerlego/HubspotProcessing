package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * @author Nicholas Curl
 */
public class CRMProperties extends HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final String urlBase = "/crm/v3/properties/";

    public CRMProperties() {
        super(0);
    }

    public static PropertyData getAllProperties(HttpService service, CRMObjectType type, boolean includeHidden) {
        Map<String, Object> properties = new HashMap<>();
        List<String> propertyNames = new LinkedList<>();
        try {
            JSONObject jsonObject = (JSONObject) service.getRequest(urlBase + type.getValue());
            JSONArray results = jsonObject.getJSONArray("results");
            for (Object o : results) {
                if (o instanceof JSONObject) {
                    JSONObject o1 = (JSONObject) o;
                    String name = o1.getString("name");
                    boolean hidden = o1.getBoolean("hidden");
                    if (!includeHidden && hidden) {
                        continue;
                    }
                    propertyNames.add(name);
                    properties.put(name, o1);
                }
            }
        } catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(-1);
        }
        Collections.sort(propertyNames);
        return new PropertyData(propertyNames, properties);
    }

    public static PropertyData getPropertiesByGroupName(HttpService service, CRMObjectType type, String groupName, boolean includeHidden) {
        Map<String, Object> properties = new HashMap<>();
        List<String> propertyNames = new LinkedList<>();
        try {
            JSONObject jsonObject = (JSONObject) service.getRequest(urlBase + type.getValue());
            JSONArray results = jsonObject.getJSONArray("results");
            for (Object o : results) {
                if (o instanceof JSONObject) {
                    JSONObject o1 = (JSONObject) o;
                    String name = "";
                    String jsonGroupName = "";
                    if (o1.has("groupName")) {
                        jsonGroupName = o1.getString("groupName");
                    }
                    if (jsonGroupName.equalsIgnoreCase(groupName)) {
                        if (o1.has("name")) {
                            name = o1.getString("name");
                        }
                        boolean hidden = o1.getBoolean("hidden");
                        if (!includeHidden && hidden) {
                            continue;
                        }
                        propertyNames.add(name);
                        properties.put(name, o1);
                    }
                }
            }
        } catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(-1);
        }
        Collections.sort(propertyNames);
        return new PropertyData(propertyNames, properties);
    }
}