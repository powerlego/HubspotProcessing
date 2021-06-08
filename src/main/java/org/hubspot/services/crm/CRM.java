package org.hubspot.services.crm;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.objects.crm.Deal;
import org.hubspot.services.crm.EngagementsProcessor.EngagementData;
import org.hubspot.utils.FileUtils;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.LogMarkers;
import org.hubspot.utils.exceptions.HubSpotException;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Nicholas Curl
 */
public class CRM implements Serializable {

    /**
     * The instance of the logger
     */
    private static final Logger      logger           = LogManager.getLogger(CRM.class);
    /**
     * The serial version UID for this class
     */
    private static final long        serialVersionUID = -548300711320850340L;
    private final        HttpService httpService;
    private final        RateLimiter rateLimiter;

    public CRM(HttpService httpService, final RateLimiter rateLimiter) {
        this.httpService = httpService;
        this.rateLimiter = rateLimiter;
    }

    //region Contacts
    public HashMap<Long, Contact> filterContacts(HashMap<Long, Contact> contacts) {
        try {
            return ContactService.filterContacts(contacts);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to filter contacts", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Contact> getAllContacts(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        return getContacts(propertyData);
    }

    public HashMap<Long, Contact> getAllContacts(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        return getContacts(propertyData);
    }

    public Contact getContactById(long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(String propertyGroup, long contactId, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, Contact> getUpdatedContacts(String propertyGroup,
                                                     boolean includeHiddenProperties,
                                                     long lastExecuted,
                                                     long lastFinished
    ) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        return getContacts(lastExecuted, propertyData, lastFinished);
    }

    public HashMap<Long, Contact> getUpdatedContacts(boolean includeHiddenProperties,
                                                     long lastExecuted,
                                                     long lastFinished
    ) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        return getContacts(lastExecuted, propertyData, lastFinished);
    }

    public HashMap<Long, Contact> readContactJsons() {
        try {
            return ContactService.readContactJsons();
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to read contact jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public void writeContactJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write contacts");
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.CONTACTS,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write contacts");
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    @NotNull
    private HashMap<Long, Contact> getContacts(PropertyData propertyData) {
        try {
            return ContactService.getAllContacts(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get all contacts", e);
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    @NotNull
    private HashMap<Long, Contact> getContacts(long lastExecuted, PropertyData propertyData, long lastFinished) {
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(ContactService.getCacheFolder());
        }
        try {
            return ContactService.getUpdatedContacts(httpService, propertyData, lastExecuted, lastFinished);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get updated contacts", e);
            FileUtils.deleteRecentlyUpdated(ContactService.getCacheFolder(), lastFinished);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }
    //endregion Contacts

    //region Companies
    public HashMap<Long, Company> getAllCompanies(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        return getCompanies(propertyData);
    }

    public HashMap<Long, Company> getAllCompanies(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        return getCompanies(propertyData);
    }

    public Company getCompanyById(String propertyGroup, long companyId, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get company of id {}", companyId, e);
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
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, Company> getUpdatedCompanies(String propertyGroup,
                                                      boolean includeHiddenProperties,
                                                      long lastExecuted,
                                                      long lastFinished
    ) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        return getCompanies(lastExecuted, propertyData, lastFinished);
    }

    public HashMap<Long, Company> getUpdatedCompanies(boolean includeHiddenProperties,
                                                      long lastExecuted,
                                                      long lastFinished
    ) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        return getCompanies(lastExecuted, propertyData, lastFinished);
    }

    public HashMap<Long, Company> readCompanyJsons() {
        try {
            return CompanyService.readCompanyJsons();
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to read company jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public void writeCompanyJson(boolean includeHiddenProperties) {
        PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write companies");
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeCompanyJson(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.COMPANIES,
                                                          propertyGroup,
                                                          includeHiddenProperties
        );
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write companies");
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    @NotNull
    private HashMap<Long, Company> getCompanies(PropertyData propertyData) {
        try {
            return CompanyService.getAllCompanies(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get all companies", e);
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    @NotNull
    private HashMap<Long, Company> getCompanies(long lastExecuted, PropertyData propertyData, long lastFinished) {
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(CompanyService.getCacheFolder());
        }
        try {
            return CompanyService.getUpdatedCompanies(httpService, propertyData, lastExecuted, lastFinished);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get updated companies", e);
            FileUtils.deleteRecentlyUpdated(CompanyService.getCacheFolder(), lastFinished);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }
    //endregion Companies

    //region Deals
    public ArrayList<Long> associateDeals(long contactId) {
        try {
            return DealService.associateDeals(httpService, contactId, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to associate deals for contact id {}", contactId, e);
            return new ArrayList<>();
        }
    }

    public HashMap<Long, Deal> getAllDeals(String propertyGroup, boolean includeHiddenProperties) {
        PropertyData propertyData = propertiesByGroupName(CRMObjectType.DEALS, propertyGroup, includeHiddenProperties);
        return getDeals(propertyData);
    }

    @NotNull
    private HashMap<Long, Deal> getDeals(PropertyData propertyData) {
        try {
            return DealService.getAllDeals(httpService, propertyData, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get all deals", e);
            FileUtils.deleteDirectory(DealService.getCacheFolder());
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }
    //endregion Deals

    //region Engagements
    public ArrayList<Long> getContactEngagementIds(Contact contact) {
        long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagementIds(httpService, contactId);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get engagement ids for contact id {}", contactId, e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public EngagementData getContactEngagements(Contact contact) {
        long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagements(httpService, contactId);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get engagements for contact id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public EngagementData getUpdatedEngagements(Contact contact, long lastFinished) {
        long contactId = contact.getId();
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(Paths.get("./cache/engagements/" + contactId));
        }
        try {
            return EngagementsProcessor.getUpdatedEngagements(httpService, contactId, lastFinished);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(),
                         "Unable to get updated engagements for contact id {}",
                         contactId,
                         e
            );
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, EngagementData> readEngagementJsons() {
        try {
            return EngagementsProcessor.readEngagementJsons();
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to read engagement jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public void writeContactEngagementJsons(long contactId) {
        try {
            EngagementsProcessor.writeContactEngagementJsons(httpService, contactId);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write engagement for contact id {}", contactId, e);
            FileUtils.deleteDirectory(EngagementsProcessor.getCacheFolder());
            System.exit(e.getCode());
        }
    }
    //endregion Engagements

    //region Properties
    public PropertyData allProperties(CRMObjectType type, boolean includeHidden) {
        try {
            return CRMProperties.getAllProperties(httpService, type, includeHidden, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    public PropertyData propertiesByGroupName(CRMObjectType type, String propertyGroup, boolean includeHidden) {
        try {
            return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup, includeHidden, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }
    //endregion Properties
}
