package org.hubspot.services.crm;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.exceptions.HubSpotException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class CRMProperties {

    /**
     * The instance of the logger
     */
    private static final Logger logger  = LogManager.getLogger(CRMProperties.class);
    private static final String urlBase = "/crm/v3/properties/";

    static PropertyData getAllProperties(HttpService service,
                                         CRMObjectType type,
                                         boolean includeHidden,
                                         final RateLimiter rateLimiter
    ) throws HubSpotException {
        Map<String, Object> properties = new HashMap<>();
        ArrayList<String> propertyNames = new ArrayList<>();
        rateLimiter.acquire();
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
        Collections.sort(propertyNames);
        return new PropertyData(propertyNames, properties);
    }

    static PropertyData getPropertiesByGroupName(HttpService service,
                                                 CRMObjectType type,
                                                 String groupName,
                                                 boolean includeHidden,
                                                 final RateLimiter rateLimiter
    ) throws HubSpotException {
        Map<String, Object> properties = new HashMap<>();
        ArrayList<String> propertyNames = new ArrayList<>();
        rateLimiter.acquire();
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
        Collections.sort(propertyNames);
        return new PropertyData(propertyNames, properties);
    }
}
