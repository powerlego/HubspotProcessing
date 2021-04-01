package org.hubspot.utils;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class HubSpotUtils {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public static JSONObject getJsonObject(String property, Object value) {
        return new JSONObject()
                .put("property", property)
                .put("value", value);
    }

    public static void putJsonObject(JSONArray ja, String property, String value){
        if(!Strings.isNullOrEmpty(value) && !value.equals("null")){
            ja.put(getJsonObject(property, value));
        }
    }

    public static JSONObject putJsonObject(JSONObject jo, String property, Object value){
        if(!Strings.isNullOrEmpty(value + "") && !value.equals("null")){
            jo.put(property, value);
        }

        return jo;
    }

    public static String mapToJsonString(Map<String, String> map) {
        return mapToJson(map).toString();
    }

    public static JSONObject mapToJson(Map<String, String> map) {
        JSONObject jo = new JSONObject(map);
        return new JSONObject().put("properties", jo);
    }
}
