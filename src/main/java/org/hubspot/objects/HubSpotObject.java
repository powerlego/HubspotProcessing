package org.hubspot.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * @author Nicholas Curl
 */
public class HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger     logger = LogManager.getLogger();
    private final        long       id;
    private              JSONObject data;

    public HubSpotObject(long id) {
        this.id = id;
    }

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public String toJSONString() {
        return data.toString();
    }
}
