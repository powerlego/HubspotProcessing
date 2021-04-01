package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.HubSpotUtils;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class HSContact {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private HashMap<String, String> properties = new HashMap<>();
    private long id;
    private List<Long> engagementIds;

    public HSContact() {
    }

    public void addEngagementId(long engagementId) {
        this.engagementIds.add(engagementId);
    }

    public String getEmail() {
        return this.properties.get("email");
    }

    public List<Long> getEngagementIds() {
        return engagementIds;
    }

    public void setEngagementIds(List<Long> engagementIds) {
        this.engagementIds = engagementIds;
    }

    public String getFirstname() {
        return this.properties.get("firstname");
    }

    public void setFirstname(String firstname) {
        this.properties.put("firstname", firstname);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLastname() {
        return this.properties.get("lastname");
    }

    public void setLastname(String lastname) {
        this.properties.put("lastname", lastname);
    }

    public String getProperty(String property) {
        return this.properties.get(property);
    }

    public void setProperty(String property, String value) {
        if (value != null) {
            if (value.isBlank()) {
                this.properties.put(property, null);
            } else {
                this.properties.put(property, value);
            }
        } else {
            this.properties.put(property, null);
        }
    }

    public String toJsonString() {
        return toJson().toString();
    }

    public JSONObject toJson() {
        HashMap<String, String> properties = new HashMap<>(getProperties());
        properties.remove("id");
        return HubSpotUtils.mapToJson(properties);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ID: ").append(id).append("\n");
        builder.append("Properties {\n");
        for (Iterator<String> iterator = properties.keySet().iterator(); iterator.hasNext(); ) {
            String key = iterator.next();
            String property = properties.get(key);
            if(!iterator.hasNext()){
                builder.append("\t").append(key).append(" = ").append(property).append("\n");
            }else {
                builder.append("\t").append(key).append(" = ").append(property).append(",\n");
            }
        }
        builder.append("}\nEngagement IDs {\n");
        for (Iterator<Long> iterator = engagementIds.iterator(); iterator.hasNext(); ) {
            long engagementId = iterator.next();
            if(!iterator.hasNext()){
                builder.append("\t").append(engagementId).append(",\n");
            } else {
                builder.append("\t").append(engagementId).append("\n");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
