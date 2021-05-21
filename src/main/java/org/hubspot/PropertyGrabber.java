package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Nicholas Curl
 */
public class PropertyGrabber {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(PropertyGrabber.class);

    public static void main(String[] args) {
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        PropertyData propertyData = hubspot.crm().allProperties(CRMObjectType.DEALS, true);
        JSONObject jsonNames = new JSONObject();
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        for (Object value : propertyData.getProperties().values()) {
            JSONObject jsonObject1 = new JSONObject();
            if (value instanceof JSONObject) {
                JSONObject object = (JSONObject) value;
                String name = object.getString("name");
                String label = object.getString("label");
                String type = object.getString("type");
                String description = object.getString("description");
                String groupName = object.getString("groupName");
                String fieldType = object.getString("fieldType");
                JSONArray options = object.getJSONArray("options");
                jsonObject1.put("name", name);
                jsonObject1.put("label", label);
                jsonObject1.put("type", type);
                jsonObject1.put("fieldType", fieldType);
                jsonObject1.put("description", description);
                jsonObject1.put("groupName", groupName);
                jsonObject1.put("options", options);
            }
            jsonArray.put(jsonObject1);
        }
        jsonObject.put("properties", jsonArray);
        jsonNames.put("names", propertyData.getPropertyNames());
        Path correctionFile = Paths.get("./contacts/deal_properties.json");
        Path propertyNames = Paths.get("./contacts/property_names.json");
        try {
            FileUtils.writeFile(correctionFile, jsonObject.toString(4));
        }
        catch (IOException e) {
            logger.fatal("Unable to write file {}", correctionFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
        try {
            FileUtils.writeFile(propertyNames, jsonNames.toString(4));
        }
        catch (IOException e) {
            logger.fatal("Unable to write file {}", propertyNames, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
