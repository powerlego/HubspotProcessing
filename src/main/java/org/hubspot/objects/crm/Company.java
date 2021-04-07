package org.hubspot.objects.crm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * @author Nicholas Curl
 */
public class Company extends CRMObject {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private String name;

    public Company(long id) {
        super(id);
    }

    public String getName() {
        return name;
    }

    public void setData() {
        super.setData(toJson());
    }

    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", getId());
        jsonObject.put("properties", super.getProperties());
        return jsonObject;
    }

    @Override
    public void setProperty(String property, Object value) {
        if (property.equalsIgnoreCase("name")) {
            this.name = value.toString();
        }
        super.setProperty(property, value);
    }
}
