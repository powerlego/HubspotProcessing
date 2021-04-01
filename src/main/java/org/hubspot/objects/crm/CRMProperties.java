package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.services.HttpService;
import org.hubspot.utils.HubSpotException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class CRMProperties {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final HttpService service;
    private final List<String> propertyNames;
    private final Map<String, Object> properties;
    private final String url;

    public CRMProperties(HttpService service, Types type) {
        this.service = service;
        this.url = "/crm/v3/properties/" + type.getValue();
        this.propertyNames = new LinkedList<>();
        this.properties = new HashMap<>();
        getAllProperties();
    }

    private void getAllProperties() {
        try {
            JSONObject jsonObject = (JSONObject) service.getRequest(url);
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
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

    public List<String> getPropertyNames() {
        return propertyNames;
    }

    public enum Types {
        CONTACTS("CONTACTS"),
        COMPANIES("COMPANIES"),
        DEALS("DEALS"),
        TICKETS("TICKETS");

        private final String value;

        Types(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
