package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    public Map<Long, Contact> filterContacts(Map<Long, Contact> contacts) {
        return ContactService.filterContacts(contacts);
    }

    public Map<Long, Company> getAllCompanies(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            return CompanyService.getAllCompanies(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new HashMap<>();
        }
    }

    public PropertyData propertiesByGroupName(CRMObjectType type, String propertyGroup, boolean includeHidden) {
        return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup, includeHidden);
    }

    public Map<Long, Company> getAllCompanies(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getAllCompanies(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new HashMap<>();
        }
    }

    public PropertyData allProperties(CRMObjectType type, boolean includeHidden) {
        return CRMProperties.getAllProperties(httpService, type, includeHidden);
    }

    public Map<Long, Contact> getAllContacts(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new HashMap<>();
        }
    }

    public Map<Long, Contact> getAllContacts(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            return new HashMap<>();
        }
    }

    public Company getCompanyById(String propertyGroup, long id, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company");
            return null;
        }
    }

    public Company getCompanyById(long id, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company");
            return null;
        }
    }

    public Contact getContactById(long id, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public Contact getContactById(String propertyGroup, long id, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id " + id, e);
            return null;
        }
    }

    public List<Long> getContactEngagementIds(Contact contact) {
        long id = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagementIds(httpService, id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get engagement ids for id " + id, e);
            return new LinkedList<>();
        }
    }

    public EngagementsProcessor.EngagementData getContactEngagements(Contact contact) {
        long id = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagements(httpService, id);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get engagements for id " + id, e);
            return null;
        }
    }

    public Map<Long, Company> readCompanyJsons() {
        return CompanyService.readCompanyJsons();
    }

    public Map<Long, Contact> readContactJsons() {
        return ContactService.readContactJsons();
    }

    public void writeCompanyJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
        }

    }

    public void writeCompanyJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
        }

    }

    public void writeContactJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }

    public void writeContactJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
        }

    }
}
