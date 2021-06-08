package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.Date;

/**
 * @author Nicholas Curl
 */
public class Deal extends CRMObject {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(Deal.class);
    private static final long   serialVersionUID = -8122694028740425205L;
    private              double amount;
    private              Date   closeDate;
    private              Date   createDate;
    private              String dealName;
    private              String dealStage;
    private              String pipeline;
    private              long   agnesID;

    /**
     * A constructor for a CRM Object
     *
     * @param id The object's Hubspot id
     */
    public Deal(long id) {
        super(id);
    }

    public long getAgnesID() {
        return agnesID;
    }

    public double getAmount() {
        return amount;
    }

    public Date getCloseDate() {
        return closeDate;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public String getPipeline() {
        return pipeline;
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

    public String getDealName() {
        return dealName;
    }

    public String getDealStage() {
        return dealStage;
    }

    /**
     * Sets/Adds a property to the company
     *
     * @param property The property to set/add
     * @param value    The value of the property
     */
    @Override
    public void setProperty(String property, Object value) {
        if (property.equalsIgnoreCase("agnes_job__")) {
            if (value instanceof Long) {
                agnesID = (long) value;
            }
            else if (value instanceof Integer) {
                agnesID = (int) value;
            }
            else {
                agnesID = -1;
            }
        }
        else if (property.equalsIgnoreCase("amount")) {
            if (value instanceof Long) {
                amount = (long) value;
            }
            else {
                amount = Long.MIN_VALUE;
            }
        }
        else if (property.equalsIgnoreCase("dealstage")) {
            dealStage = value.toString();
        }
        else if (property.equalsIgnoreCase("dealname")) {
            dealName = value.toString();
        }
        else if (property.equalsIgnoreCase("pipeline")) {
            pipeline = value.toString();
        }
        else if (property.equalsIgnoreCase("createdate")) {
            if (value instanceof Date) {
                createDate = (Date) value;
            }
            else {
                createDate = null;
            }
        }
        else if (property.equalsIgnoreCase("closedate")) {
            if (value instanceof Date) {
                closeDate = (Date) value;
            }
            else {
                closeDate = null;
            }
        }
        super.setProperty(property, value);
    }
}
