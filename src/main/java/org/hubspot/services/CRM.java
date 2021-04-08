package org.hubspot.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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

    public ArrayList<Contact> filterContacts(ArrayList<Contact> contacts) {
        return ContactService.filterContacts(contacts);
    }

    public ConcurrentHashMap<Long, Company> getAllCompanies(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            return CompanyService.getAllCompanies(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            System.exit(e.getCode());
            return new ConcurrentHashMap<>();
        }
    }

    public PropertyData propsByGroupName(CRMObjectType type, String propertyGroup, boolean includeHidden) {
        try {
            return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup, includeHidden);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    public ConcurrentHashMap<Long, Company> getAllCompanies(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getAllCompanies(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            System.exit(e.getCode());
            return new ConcurrentHashMap<>();
        }
    }

    public PropertyData allProperties(CRMObjectType type, boolean includeHidden) {
        try {
            return CRMProperties.getAllProperties(httpService, type, includeHidden);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    public ArrayList<Contact> getAllContacts(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData =
                propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public ArrayList<Contact> getAllContacts(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getAllContacts(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public Company getCompanyById(String propertyGroup, long companyId, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Company getCompanyById(long companyId, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(String propertyGroup, long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public List<Long> getContactEngagementIds(Contact contact) {
        long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagementIds(httpService, contactId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get engagement ids for contact id {}", contactId, e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public EngagementsProcessor.EngagementData getContactEngagements(Contact contact) {
        long contactId = contact.getId();

        try {
            return EngagementsProcessor.getAllEngagements(httpService, contactId);
        } catch (HubSpotException e) {
            logger.fatal("Unable to get engagements for contact id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public ConcurrentHashMap<Long, Company> readCompanyJsons() {
        return CompanyService.readCompanyJsons();
    }

    public ArrayList<Contact> readContactJsons() {
        return ContactService.readContactJsons();
    }

    public void writeCompanyJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
            System.exit(e.getCode());
        }

    }

    public void writeCompanyJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
            System.exit(e.getCode());
        }

    }

    public void writeContactJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
            System.exit(e.getCode());
        }

    }

    public void writeContactJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData);
        } catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
            System.exit(e.getCode());
        }

    }
}
