package org.hubspot.utils;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.utils.exceptions.HubSpotException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

/**
 * @author Nicholas Curl
 */
public class HubSpotUtils {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(HubSpotUtils.class);

    public static long getObjectCount(HttpService service, CRMObjectType type, final RateLimiter rateLimiter) {
        JSONObject body = new JSONObject();
        JSONArray filterGroupsArray = new JSONArray();
        JSONObject filters = new JSONObject();
        JSONArray filtersArray = new JSONArray();
        JSONObject filter = new JSONObject();
        JSONArray propertyArray = new JSONArray(PropertyData.EMPTY.getPropertyNames());
        filter.put("propertyName", "hs_object_id").put("operator", "HAS_PROPERTY");
        filtersArray.put(filter);
        filters.put("filters", filtersArray);
        filterGroupsArray.put(filters);
        body.put("filterGroups", filterGroupsArray).put("properties", propertyArray).put("limit", 1);
        return getCount(service, type, rateLimiter, body);
    }

    private static long getCount(HttpService service,
                                 CRMObjectType type,
                                 final RateLimiter rateLimiter,
                                 JSONObject body
    ) {
        try {
            rateLimiter.acquire(1);
            JSONObject resp = (JSONObject) service.postRequest("/crm/v3/objects/" + type.getValue() + "/search", body);
            return resp.getLong("total");
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get object count.", e);
            return 0;
        }
    }

    public static long getUpdateCount(HttpService service,
                                      final RateLimiter rateLimiter,
                                      CRMObjectType type,
                                      long lastExecution
    ) {
        JSONObject body = getUpdateBody(type, PropertyData.EMPTY, lastExecution, 1);
        return getCount(service, type, rateLimiter, body);
    }

    public static JSONObject getUpdateBody(CRMObjectType type,
                                           PropertyData propertyData,
                                           long lastExecution,
                                           int limit
    ) {
        JSONObject body = new JSONObject();
        JSONArray filterGroupsArray = new JSONArray();
        JSONObject filters1 = new JSONObject();
        JSONArray filtersArray1 = new JSONArray();
        JSONObject filter1 = new JSONObject();
        JSONObject filters2 = new JSONObject();
        JSONArray filtersArray2 = new JSONArray();
        JSONObject filter2 = new JSONObject();
        filter1.put("propertyName", "createdate").put("operator", "GTE").put("value", lastExecution);
        filtersArray1.put(filter1);
        filters1.put("filters", filtersArray1);
        switch (type) {
            case CONTACTS:
                filter2.put("propertyName", "lastmodifieddate").put("operator", "GTE").put("value", lastExecution);
                break;
            case COMPANIES:
            case DEALS:
            case TICKETS:
                filter2.put("propertyName", "hs_lastmodifieddate").put("operator", "GTE").put("value", lastExecution);
                break;
        }
        filtersArray2.put(filter2);
        filters2.put("filters", filtersArray2);
        filterGroupsArray.put(filters1).put(filters2);
        JSONArray propertyArray = new JSONArray(propertyData.getPropertyNames());
        body.put("filterGroups", filterGroupsArray).put("properties", propertyArray).put("limit", limit);
        return body;
    }

    public static String propertyListToString(List<String> properties) {
        return properties.toString().replace("[", "").replace("]", "").replace(" ", "");
    }
}
