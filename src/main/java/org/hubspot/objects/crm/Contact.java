package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.Engagement;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Contact extends CRMObject {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private List<Long> engagementIds = new LinkedList<>();
    private List<Engagement> engagements = new LinkedList<>();
    private String leadStatus;
    private String lifeCycleStage;
    private String firstName;
    private String lastName;
    private String email;

    public Contact(long id) {
        super(id);
    }

    public void addEngagement(Engagement engagement) {
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

    public long getAssociatedCompany() {
        Object companyIdObject = this.getProperties().get("associatedcompanyid");
        if (companyIdObject instanceof Integer) {
            return (int) companyIdObject;
        }
        return (long) this.getProperties().get("associatedcompanyid");
    }

    public List<Engagement> getEngagements() {
        return engagements;
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

    public void setEngagements(List<Engagement> engagements) {
        this.engagements = engagements;
    }

    @Override
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
        super.setProperty(property, value);
    }

    public String toJsonString() {
        return toJson().toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString());
        builder.append("\nEngagement IDs {\n");
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

    public JSONObject toJson() {
        JSONObject jo = new JSONObject(super.getProperties());
        JSONArray ja = new JSONArray(engagementIds);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("class", this.getClass().getName());
        jsonObject.put("id", getId());
        jsonObject.put("properties", jo);
        jsonObject.put("engagements", ja);
        return jsonObject;
    }
}
