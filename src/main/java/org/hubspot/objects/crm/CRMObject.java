package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class CRMObject extends HubSpotObject {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private HashMap<String, Object> properties = new HashMap<>();

    public CRMObject(long id) {
        super(id);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, Object> properties) {
        this.properties = properties;
    }

    public Object getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    public void setProperty(String property, Object value) {
        this.properties.put(property, value);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ID: ").append(getId()).append("\n");
        builder.append("Properties {\n");
        for (Iterator<String> iterator = properties.keySet().iterator(); iterator.hasNext(); ) {
            String key = iterator.next();
            Object property = properties.get(key);
            if (!iterator.hasNext()) {
                builder.append("\t")
                        .append(key)
                        .append(" = ")
                        .append(property == null ? "null" : property)
                        .append("\n");
            } else {
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
