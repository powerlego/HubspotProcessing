package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMProperties;
import org.hubspot.objects.crm.CRMProperties.PropertyData;
import org.hubspot.objects.crm.Contact;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class CRM {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final HttpService httpService;

    public CRM(HttpService httpService) {
        this.httpService = httpService;
    }

    public List<Contact> getAllContacts() {
        PropertyData propertyData = allContactProperties();
        try {
            return ContactService.getAllContacts(httpService, propertyData.getPropertyNamesString());
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public PropertyData allContactProperties() {
        return CRMProperties.getAllProperties(httpService, CRMObjectType.CONTACTS);
    }

    public List<Contact> getAllContacts(String propertyGroup) {
        PropertyData propertyData = contactPropertiesByGroupName(propertyGroup);
        try {
            return ContactService.getAllContacts(httpService, propertyData.getPropertyNamesString());
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public PropertyData contactPropertiesByGroupName(String propertyGroup) {
        return CRMProperties.getPropertiesByGroupName(httpService, CRMObjectType.CONTACTS, propertyGroup);
    }
}
