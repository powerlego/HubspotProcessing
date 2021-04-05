package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.CRMProperties;
import org.hubspot.objects.crm.CRMProperties.PropertyData;
import org.hubspot.objects.crm.Contact;
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

    public List<Contact> filterContacts(List<Contact> contacts) {
        return ContactService.filterContacts(contacts);
    }

    public Contact getContactById(long id) {
        PropertyData propertyData = allContactProperties();
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public Contact getContactById(String propertyGroup, long id) {
        PropertyData propertyData = contactPropertiesByGroupName(propertyGroup);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public List<Contact> getFilteredContacts(String propertyGroup) {
        List<Contact> allContacts = getAllContacts(propertyGroup);
        return ContactService.filterContacts(allContacts);
    }

    public PropertyData allContactProperties() {
        return CRMProperties.getAllProperties(httpService, CRMObjectType.CONTACTS);
    }

    public List<Contact> getAllContacts(String propertyGroup) {
        PropertyData propertyData = contactPropertiesByGroupName(propertyGroup);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public List<Contact> getFilteredContacts() {
        List<Contact> allContacts = getAllContacts();
        return ContactService.filterContacts(allContacts);
    }

    public List<Contact> getAllContacts() {
        PropertyData propertyData = allContactProperties();
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public List<Contact> readContactJsons() {
        return ContactService.readContactJsons();
    }

    public void writeContactJson() {
        PropertyData propertyData = allContactProperties();
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }

    public PropertyData contactPropertiesByGroupName(String propertyGroup) {
        return CRMProperties.getPropertiesByGroupName(httpService, CRMObjectType.CONTACTS, propertyGroup);
    }

    public void writeContactJson(String propertyGroup) {
        PropertyData propertyData = contactPropertiesByGroupName(propertyGroup);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }
}
