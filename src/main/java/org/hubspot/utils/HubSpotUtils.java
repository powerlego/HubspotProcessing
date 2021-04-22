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
    private static final Logger logger = LogManager.getLogger();

    public static long getObjectCount(final HttpService service,
                                      final CRMObjectType type,
                                      final RateLimiter rateLimiter
    ) {
        final JSONObject body = new JSONObject();
        final JSONArray filterGroupsArray = new JSONArray();
        final JSONObject filters = new JSONObject();
        final JSONArray filtersArray = new JSONArray();
        final JSONObject filter = new JSONObject();
        final JSONArray propertyArray = new JSONArray(PropertyData.EMPTY.getPropertyNames());
        filter.put("propertyName", "hs_object_id").put("operator", "HAS_PROPERTY");
        filtersArray.put(filter);
        filters.put("filters", filtersArray);
        filterGroupsArray.put(filters);
        body.put("filterGroups", filterGroupsArray).put("properties", propertyArray).put("limit", 1);
        return getCount(service, type, rateLimiter, body);
    }

    private static long getCount(final HttpService service,
                                 final CRMObjectType type,
                                 final RateLimiter rateLimiter,
                                 final JSONObject body
    ) {
        try {
            rateLimiter.acquire(1);
            final JSONObject resp = (JSONObject) service.postRequest("/crm/v3/objects/" + type.getValue() + "/search",
                                                                     body
            );
            return resp.getLong("total");
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get object count.", e);
            return 0;
        }
    }

    public static long getUpdateCount(final HttpService service,
                                      final RateLimiter rateLimiter,
                                      final CRMObjectType type,
                                      final long lastExecution
    ) {
        final JSONObject body = getUpdateBody(type, PropertyData.EMPTY, lastExecution, 1);
        return getCount(service, type, rateLimiter, body);
    }

    public static JSONObject getUpdateBody(final CRMObjectType type,
                                           final PropertyData propertyData,
                                           final long lastExecution,
                                           final int limit
    ) {
        final JSONObject body = new JSONObject();
        final JSONArray filterGroupsArray = new JSONArray();
        final JSONObject filters1 = new JSONObject();
        final JSONArray filtersArray1 = new JSONArray();
        final JSONObject filter1 = new JSONObject();
        final JSONObject filters2 = new JSONObject();
        final JSONArray filtersArray2 = new JSONArray();
        final JSONObject filter2 = new JSONObject();
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
        final JSONArray propertyArray = new JSONArray(propertyData.getPropertyNames());
        body.put("filterGroups", filterGroupsArray).put("properties", propertyArray).put("limit", limit);
        return body;
    }

    public static String propertyListToString(final List<String> properties) {
        return properties.toString().replace("[", "").replace("]", "").replace(" ", "");
    }

}
