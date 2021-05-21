package org.hubspot.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.Serializable;

/**
 * A class that represents a Hubspot Object
 *
 * @author Nicholas Curl
 */
public class HubSpotObject implements Serializable {

    /**
     * The instance of the logger
     */
    private static final Logger     logger           = LogManager.getLogger(HubSpotObject.class);
    /**
     * The serial version UID for this class
     */
    private static final long       serialVersionUID = 258223066597403304L;
    /**
     * The Hubspot object id
     */
    private final        long       id;
    /**
     * The Json data of the Hubspot object
     */
    private              JSONObject data;

    /**
     * The constructor of this object
     *
     * @param id The Hubspot object Id
     */
    public HubSpotObject(long id) {
        this.id = id;
    }

    /**
     * Gets the Json data of this object
     *
     * @return The Json data of this object
     */
    public JSONObject getData() {
        return data;
    }

    /**
     * Sets the Json data for this object
     *
     * @param data The Json data to set
     */
    public void setData(JSONObject data) {
        this.data = data;
    }

    /**
     * Gets the object's id
     *
     * @return The object's id
     */
    public long getId() {
        return id;
    }

    /**
     * Returns the string representation of the Json data
     *
     * @return The string representation of the Json data
     */
    public String toJSONString() {
        return data.toString();
    }
}
