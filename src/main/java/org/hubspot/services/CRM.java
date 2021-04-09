package org.hubspot.services;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.EngagementsProcessor.EngagementData;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.HubSpotException;
import org.hubspot.utils.Utils;
import org.jetbrains.annotations.NotNull;

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
    private static final Logger      logger = LogManager.getLogger();
    private final        HttpService httpService;
    private final        RateLimiter rateLimiter;

    public CRM(HttpService httpService, RateLimiter rateLimiter) {
        this.httpService = httpService;
        this.rateLimiter = rateLimiter;
    }

    public ArrayList<Contact> filterContacts(ArrayList<Contact> contacts) {
        return ContactService.filterContacts(contacts);
    }

    public ConcurrentHashMap<Long, Company> getAllCompanies(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        return getCompanies(propertyData);
    }

    public PropertyData propsByGroupName(CRMObjectType type, String propertyGroup, boolean includeHidden) {
        try {
            return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup, includeHidden, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    @NotNull
    private ConcurrentHashMap<Long, Company> getCompanies(PropertyData propertyData) {
        try {
            return CompanyService.getAllCompanies(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get all companies", e);
            Utils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
            return new ConcurrentHashMap<>();
        }
    }

    public ConcurrentHashMap<Long, Company> getAllCompanies(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        return getCompanies(propertyData);
    }

    public PropertyData allProperties(CRMObjectType type, boolean includeHidden) {
        try {
            return CRMProperties.getAllProperties(httpService, type, includeHidden, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    public ArrayList<Contact> getAllContacts(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        return getContacts(propertyData);
    }

    @NotNull
    private ArrayList<Contact> getContacts(PropertyData propertyData) {
        try {
            return ContactService.getAllContacts(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            Utils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public ArrayList<Contact> getAllContacts(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        return getContacts(propertyData);
    }

    public Company getCompanyById(String propertyGroup, long companyId, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Company getCompanyById(long companyId, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(String propertyGroup, long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public List<Long> getContactEngagementIds(Contact contact) {
        long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagementIds(httpService, contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get engagement ids for contact id {}", contactId, e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public EngagementData getContactEngagements(Contact contact) {
        long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagements(httpService, contactId, rateLimiter);
        }
        catch (HubSpotException e) {
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

    public ConcurrentHashMap<Long, EngagementData> readEngagementJsons() {
        return EngagementsProcessor.readEngagementJsons();
    }

    public void writeCompanyJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
            Utils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeCompanyJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES, propertyGroup, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to write companies");
            Utils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactEngagementJsons(long contactId) {
        try {
            EngagementsProcessor.writeContactEngagementJsons(httpService, contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to write engagement for contact id {}", contactId, e);
            Utils.deleteDirectory(EngagementsProcessor.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
            Utils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS, propertyGroup, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to write contacts");
            Utils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }
}
