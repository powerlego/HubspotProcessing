package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
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

    public static PropertyData getAllProperties(HttpService service, CRMObjectType type) {
        Map<String, Object> properties = new HashMap<>();
        List<String> propertyNames = new LinkedList<>();
        try {
            JSONObject jsonObject = (JSONObject) service.getRequest(urlBase + type.getValue());
            JSONArray results = jsonObject.getJSONArray("results");
            for (Object o : results) {
                if (o instanceof JSONObject) {
                    JSONObject o1 = (JSONObject) o;
                    String name = o1.getString("name");
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

    public static PropertyData getPropertiesByGroupName(HttpService service, CRMObjectType type, String groupName) {
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

    public static class PropertyData {
        private final List<String> propertyNames;
        private final Map<String, Object> properties;

        public PropertyData(List<String> propertyNames, Map<String, Object> properties) {
            this.properties = properties;
            this.propertyNames = propertyNames;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public Object getProperty(String propertyName) {
            return properties.get(propertyName);
        }

        public List<String> getPropertyNames() {
            return propertyNames;
        }

        public String getPropertyNamesString() {
            return Utils.propertyListToString(propertyNames);
        }
    }
}
