package org.hubspot.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.HubSpotUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class PropertyData implements Serializable {

    /**
     * The instance of the logger
     */
    private static final Logger              logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long                serialVersionUID = -6380123557928383335L;
    /**
     * An empty property data
     */
    public static        PropertyData        EMPTY
                                                              = new PropertyData(new ArrayList<>(List.of("hs_object_id")),
                                                                                 new HashMap<>()
    );
    /**
     * The list of property names
     */
    private final        ArrayList<String>   propertyNames;
    /**
     * The map of properties
     */
    private final        Map<String, Object> properties;

    /**
     * A constructor for a Hubspot Object property data
     *
     * @param propertyNames The property names
     * @param properties    The map of properties
     */
    public PropertyData(ArrayList<String> propertyNames, Map<String, Object> properties) {
        this.properties = properties;
        this.propertyNames = propertyNames;
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
     * Gets the value of the specified property
     *
     * @param propertyName The property to get the value of
     *
     * @return The value of the property
     */
    public Object getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    /**
     * Gets the list of property names
     *
     * @return The list of property names
     */
    public ArrayList<String> getPropertyNames() {
        return propertyNames;
    }

    /**
     * Returns the string representation of the property names
     *
     * @return The string representation of the property names
     */
    public String getPropertyNamesString() {
        return HubSpotUtils.propertyListToString(propertyNames);
    }
}
