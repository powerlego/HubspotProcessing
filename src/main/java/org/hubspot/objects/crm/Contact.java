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
 * A class to represent a Contact object
 *
 * @author Nicholas Curl
 */
public class Contact extends CRMObject {

    /**
     * The instance of the logger
     */
    private static final Logger           logger           = LogManager.getLogger(Contact.class);
    /**
     * The serial version UID for this class
     */
    private static final long             serialVersionUID = 381028750506128910L;
    /**
     * The list of engagement ids associated with the contact
     */
    private              List<Long>       engagementIds    = new LinkedList<>();
    /**
     * The list of engagements associated with the contact
     */
    private              List<Engagement> engagements      = new LinkedList<>();
    /**
     * The lead status of the contact
     */
    private              String           leadStatus;
    /**
     * The life cycle stage of the contact
     */
    private              String           lifeCycleStage;
    /**
     * The first name of the contact
     */
    private              String           firstName;
    /**
     * The last name of the contact
     */
    private              String           lastName;
    /**
     * The contact's email address
     */
    private              String           email;

    /**
     * A constructor for a Contact object
     *
     * @param id The contact's Hubspot id
     */
    public Contact(long id) {
        super(id);
    }

    /**
     * Associates a list of engagement ids with this contact
     *
     * @param engagementIds The list of engagement ids to associate
     */
    public void addAllEngagementIds(List<Long> engagementIds) {
        this.engagementIds.addAll(engagementIds);
    }

    /**
     * Associates a list of engagements with this contact
     *
     * @param engagements The list of engagements to associate
     */
    public void addAllEngagements(List<Engagement> engagements) {
        this.engagements.addAll(engagements);
    }

    /**
     * Associate an engagement with this contact
     *
     * @param engagement The engagement to associate
     */
    public void addEngagement(Engagement engagement) {
        this.engagements.add(engagement);
    }

    /**
     * Associate an engagement id with this contact
     *
     * @param engagementId The engagement id to associate
     */
    public void addEngagementId(long engagementId) {
        this.engagementIds.add(engagementId);
    }

    /**
     * Gets the associated company id of this contact
     *
     * @return The associated company id
     */
    public long getAssociatedCompany() {
        Object companyIdObject = this.getProperties().get("associatedcompanyid");
        if (companyIdObject instanceof Integer) {
            return (int) companyIdObject;
        }
        if (companyIdObject.toString().equalsIgnoreCase("null")) {
            return 0;
        }
        return (long) this.getProperties().get("associatedcompanyid");
    }

    /**
     * Gets the email address of this contact
     *
     * @return The email address of this contact
     */
    public String getEmail() {
        return email;
    }

    /**
     * The list of engagement ids associated with this contact
     *
     * @return The list of engagement ids associated with contact
     */
    public List<Long> getEngagementIds() {
        return engagementIds;
    }

    /**
     * Sets a list of engagement ids that are associated with this contact
     *
     * @param engagementIds The list of engagement ids that are associated with this contact
     */
    public void setEngagementIds(List<Long> engagementIds) {
        this.engagementIds = engagementIds;
    }

    /**
     * Gets the list of engagements associated with this contact
     *
     * @return The list of engagements associated with this contact
     */
    public List<Engagement> getEngagements() {
        return engagements;
    }

    /**
     * Sets a list of engagements that are associated with this contact
     *
     * @param engagements The list of engagements that are associated with this contact
     */
    public void setEngagements(List<Engagement> engagements) {
        this.engagements = engagements;
    }

    /**
     * Gets the first name of this contact
     *
     * @return The first name of this contact
     */
    public String getFirstName() {
        return firstName;
    }

    /**
     * Gets the last name of this contact
     *
     * @return The last name of this contact
     */
    public String getLastName() {
        return lastName;
    }

    /**
     * Gets the lead status of this contact
     *
     * @return The lead status of this contact
     */
    public String getLeadStatus() {
        return leadStatus;
    }

    /**
     * Gets the life cycle stage of this contact
     *
     * @return The life cycle stage of this contact
     */
    public String getLifeCycleStage() {
        return lifeCycleStage;
    }

    /**
     * Sets/Adds a property to the contact
     *
     * @param property The property to set/add
     * @param value    The value of the property
     */
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

    /**
     * Returns the string representation of this contact
     *
     * @return The string representation of this contact
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString()).append("\nEngagement IDs {\n");
        for (Iterator<Long> iterator = engagementIds.iterator(); iterator.hasNext(); ) {
            long engagementId = iterator.next();
            if (!iterator.hasNext()) {
                builder.append("\t").append(engagementId).append("\n");
            }
            else {
                builder.append("\t").append(engagementId).append(",\n");
            }
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Converts this contact into a json object
     *
     * @return The json object representation of this contact
     */
    public JSONObject toJson() {
        JSONObject jo = new JSONObject(super.getProperties());
        JSONArray ja = new JSONArray(engagementIds);
        return new JSONObject().put("class", this.getClass().getName())
                               .put("id", getId())
                               .put("properties", jo)
                               .put("engagements", ja);
    }
}
