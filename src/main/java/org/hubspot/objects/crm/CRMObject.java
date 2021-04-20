package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A class to represent a Hubspot CRM object
 *
 * @author Nicholas Curl
 */
public class CRMObject extends HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger                  logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long                    serialVersionUID = 74879428395172470L;
    /**
     * A map of properties for the CRM object
     */
    private              HashMap<String, Object> properties       = new HashMap<>();

    /**
     * A constructor for a CRM Object
     *
     * @param id The object's Hubspot id
     */
    public CRMObject(long id) {
        super(id);
    }

    /**
     * Adds a map of properties to the object's property map
     *
     * @param properties The map of properties to add
     */
    public void addAllProperties(HashMap<String, Object> properties) {
        this.properties.putAll(properties);
    }

    /**
     * Gets the map of properties
     *
     * @return The map of properties
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Sets the properties of the CRM object
     *
     * @param properties The properties to set
     */
    public void setProperties(HashMap<String, Object> properties) {
        this.properties = properties;
    }

    /**
     * Gets the value of the specified property name
     *
     * @param propertyName The property name to grab the value
     *
     * @return The value of the property
     */
    public Object getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    /**
     * Sets/Adds a property to the CRM object
     *
     * @param property The property to set/add
     * @param value    The value of the property
     */
    public void setProperty(String property, Object value) {
        this.properties.put(property, value);
    }

    /**
     * Returns the string representation of the CRM object
     *
     * @return The string representation of the CRM object
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append("ID: ")
                                                   .append(getId())
                                                   .append("\n")
                                                   .append("Properties {\n");
        for (Iterator<String> iterator = properties.keySet().iterator(); iterator.hasNext(); ) {
            String key = iterator.next();
            Object property = properties.get(key);
            if (!iterator.hasNext()) {
                builder.append("\t")
                       .append(key)
                       .append(" = ")
                       .append(property == null ? "null" : property)
                       .append("\n");
            }
            else {
                builder.append("\t")
                       .append(key)
                       .append(" = ")
                       .append(property == null ? "null" : property)
                       .append(",\n");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
