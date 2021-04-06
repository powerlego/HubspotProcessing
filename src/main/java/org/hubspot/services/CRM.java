package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.CRMProperties;
import org.hubspot.objects.crm.CRMProperties.PropertyData;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.json.JSONObject;

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

    public List<Contact> getAllContacts(String propertyGroup) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public List<Contact> getAllContacts() {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new LinkedList<>();
        }
    }

    public JSONObject getCompanyById(String propertyGroup, long id) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES, propertyGroup);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company");
            return null;
        }
    }

    public JSONObject getCompanyById(long id) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company");
            return null;
        }
    }

    public List<Contact> getFilteredContacts(String propertyGroup) {
        List<Contact> allContacts = getAllContacts(propertyGroup);
        return ContactService.filterContacts(allContacts);
    }

    public Contact getContactById(long id) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public List<Contact> getFilteredContacts() {
        List<Contact> allContacts = getAllContacts();
        return ContactService.filterContacts(allContacts);
    }

    public PropertyData allProperties(CRMObjectType type) {
        return CRMProperties.getAllProperties(httpService, type);
    }

    public List<Contact> readContactJsons() {
        return ContactService.readContactJsons();
    }

    public Contact getContactById(String propertyGroup, long id) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public PropertyData propertiesByGroupName(CRMObjectType type, String propertyGroup) {
        return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup);
    }

    public void writeContactJson() {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }

    public void writeContactJson(String propertyGroup) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }
}
