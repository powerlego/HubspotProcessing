package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * @author Nicholas Curl
 */
public class Contact extends HubSpotObject {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private HashMap<String, Object> properties = new HashMap<>();
    private List<Long> engagementIds = new LinkedList<>();
    private List<Object> engagements = new LinkedList<>();
    private String leadStatus;
    private String lifeCycleStage;
    private String firstName;
    private String lastName;
    private String email;

    public Contact(long id) {
        super(id);
    }

    public void addEngagement(Object engagement) {
        this.engagements.add(engagement);
    }

    public void addEngagementId(long engagementId) {
        this.engagementIds.add(engagementId);
    }

    public List<Long> getEngagementIds() {
        return engagementIds;
    }

    public void setEngagementIds(List<Long> engagementIds) {
        this.engagementIds = engagementIds;
    }

    public List<Object> getEngagements() {
        return engagements;
    }

    public void setEngagements(List<Object> engagements) {
        this.engagements = engagements;
    }

    public String getEmail() {
        return email;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getLeadStatus() {
        return leadStatus;
    }

    public String getLifeCycleStage() {
        return lifeCycleStage;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, Object> properties) {
        this.properties = properties;
    }

    public Object getProperty(String property) {
        return this.properties.get(property);
    }

    public void setData() {
        super.setData(toJson());
    }

    public void setProperty(String property, Object value) {
        if (property.equalsIgnoreCase("lifecyclestage")) {
            this.lifeCycleStage = value.toString();
        }
        if (property.equalsIgnoreCase("hs_lead_status")) {
            this.leadStatus = value.toString();
        }
        if (property.equalsIgnoreCase("email")) {
            this.email = value.toString();
        }
        if (property.equalsIgnoreCase("firstname")) {
            this.firstName = value.toString();
        }
        if (property.equalsIgnoreCase("lastname")) {
            this.lastName = value.toString();
        }
        this.properties.put(property, value);
    }

    public String toJsonString() {
        return toJson().toString();
    }

    public JSONObject toJson() {
        JSONObject jo = new JSONObject(properties);
        JSONArray ja = new JSONArray(engagementIds);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("class", this.getClass().getName());
        jsonObject.put("id", getId());
        jsonObject.put("properties", jo);
        jsonObject.put("engagements", ja);
        return jsonObject;
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
                builder.append("\t").append(key).append(" = ").append(property == null ? "null" : property).append("\n");
            } else {
                builder.append("\t").append(key).append(" = ").append(property == null ? "null" : property).append(",\n");
            }
        }
        builder.append("}\nEngagement IDs {\n");
        for (Iterator<Long> iterator = engagementIds.iterator(); iterator.hasNext(); ) {
            long engagementId = iterator.next();
            if (!iterator.hasNext()) {
                builder.append("\t").append(engagementId).append("\n");
            } else {
                builder.append("\t").append(engagementId).append(",\n");
            }
        }
        builder.append("}");
        return builder.toString();
    }
}
