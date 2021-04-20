package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * A class to represent a Hubspot Company object
 *
 * @author Nicholas Curl
 */
public class Company extends CRMObject {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    /**
     * The name of the company
     */
    private              String name;

    /**
     * The constructor for this object
     *
     * @param id The Hubspot company id
     */
    public Company(long id) {
        super(id);
    }

    /**
     * Gets the company's name
     *
     * @return The company's name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the json data of this company
     *
     * @param data The json object containing the data
     */
    @Override
    public void setData(JSONObject data) {
        super.setData(data);
    }

    /**
     * Sets the json data of this company based on its json conversion
     */
    public void setData() {
        super.setData(toJson());
    }

    /**
     * Converts this company into a json object
     *
     * @return The json object representing this company
     */
    public JSONObject toJson() {
        return new JSONObject().put("id", getId()).put("properties", super.getProperties());
    }

    /**
     * Sets/Adds a property to the company
     *
     * @param property The property to set/add
     * @param value    The value of the property
     */
    @Override
    public void setProperty(String property, Object value) {
        if (property.equalsIgnoreCase("name")) {
            this.name = value.toString();
        }
        super.setProperty(property, value);
    }
}
