package org.hubspot.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.Utils;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class PropertyData {

    /**
     * The instance of the logger
     */
    private static final Logger              logger = LogManager.getLogger();
    private final        ArrayList<String>   propertyNames;
    private final        Map<String, Object> properties;

    public PropertyData(ArrayList<String> propertyNames, Map<String, Object> properties) {
        this.properties = properties;
        this.propertyNames = propertyNames;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public Object getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    public ArrayList<String> getPropertyNames() {
        return propertyNames;
    }

    public String getPropertyNamesString() {
        return Utils.propertyListToString(propertyNames);
    }
}
