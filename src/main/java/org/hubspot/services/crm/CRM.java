package org.hubspot.services.crm;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.objects.crm.Contact;
import org.hubspot.services.crm.EngagementsProcessor.EngagementData;
import org.hubspot.utils.FileUtils;
import org.hubspot.utils.HttpService;
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
    private static final Logger      logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long        serialVersionUID = -548300711320850340L;
    private final        HttpService httpService;
    private final        RateLimiter rateLimiter;

    public CRM(final HttpService httpService, final RateLimiter rateLimiter) {
        this.httpService = httpService;
        this.rateLimiter = rateLimiter;
    }

    public HashMap<Long, Contact> filterContacts(final HashMap<Long, Contact> contacts) {
        try {
            return ContactService.filterContacts(contacts);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to filter contacts", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Company> getAllCompanies(final String propertyGroup, final boolean includeHiddenProperties) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        return getCompanies(propertyData);
    }

    public PropertyData propsByGroupName(final CRMObjectType type,
                                         final String propertyGroup,
                                         final boolean includeHidden
    ) {
        try {
            return CRMProperties.getPropertiesByGroupName(httpService, type, propertyGroup, includeHidden, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    @NotNull
    private HashMap<Long, Company> getCompanies(final PropertyData propertyData) {
        try {
            return CompanyService.getAllCompanies(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get all companies", e);
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Company> getAllCompanies(final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        return getCompanies(propertyData);
    }

    public PropertyData allProperties(final CRMObjectType type, final boolean includeHidden) {
        try {
            return CRMProperties.getAllProperties(httpService, type, includeHidden, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get properties", e);
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, Contact> getAllContacts(final String propertyGroup, final boolean includeHiddenProperties) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        return getContacts(propertyData);
    }

    @NotNull
    private HashMap<Long, Contact> getContacts(final PropertyData propertyData) {
        try {
            return ContactService.getAllContacts(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get all contacts", e);
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Contact> getAllContacts(final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        return getContacts(propertyData);
    }

    public Company getCompanyById(final String propertyGroup,
                                  final long companyId,
                                  final boolean includeHiddenProperties
    ) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Company getCompanyById(final long companyId, final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            return CompanyService.getByID(httpService, propertyData.getPropertyNamesString(), companyId, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get company of id {}", companyId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(final long contactId, final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public Contact getContactById(final String propertyGroup,
                                  final long contactId,
                                  final boolean includeHiddenProperties
    ) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        try {
            return ContactService.getByID(httpService, propertyData.getPropertyNamesString(), contactId, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get contact of id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public ArrayList<Long> getContactEngagementIds(final Contact contact) {
        final long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagementIds(httpService, contactId);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get engagement ids for contact id {}", contactId, e);
            System.exit(e.getCode());
            return new ArrayList<>();
        }
    }

    public EngagementData getContactEngagements(final Contact contact) {
        final long contactId = contact.getId();
        try {
            return EngagementsProcessor.getAllEngagements(httpService, contactId);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get engagements for contact id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, Company> getUpdatedCompanies(final String propertyGroup,
                                                      final boolean includeHiddenProperties,
                                                      final long lastExecuted,
                                                      final long lastFinished
    ) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        return getCompanies(lastExecuted, propertyData, lastFinished);
    }

    @NotNull
    private HashMap<Long, Company> getCompanies(final long lastExecuted,
                                                final PropertyData propertyData,
                                                long lastFinished
    ) {
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(CompanyService.getCacheFolder());
        }
        try {
            return CompanyService.getUpdatedCompanies(httpService, propertyData, lastExecuted, lastFinished);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get updated companies", e);
            FileUtils.deleteRecentlyUpdated(CompanyService.getCacheFolder(), lastFinished);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Company> getUpdatedCompanies(final boolean includeHiddenProperties,
                                                      final long lastExecuted,
                                                      final long lastFinished
    ) {
        final PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        return getCompanies(lastExecuted, propertyData, lastFinished);
    }

    public HashMap<Long, Contact> getUpdatedContacts(final String propertyGroup,
                                                     final boolean includeHiddenProperties,
                                                     final long lastExecuted,
                                                     final long lastFinished
    ) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        return getContacts(lastExecuted, propertyData, lastFinished);
    }

    @NotNull
    private HashMap<Long, Contact> getContacts(final long lastExecuted,
                                               final PropertyData propertyData,
                                               long lastFinished
    ) {
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(ContactService.getCacheFolder());
        }
        try {
            return ContactService.getUpdatedContacts(httpService, propertyData, lastExecuted, lastFinished);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get updated contacts", e);
            FileUtils.deleteRecentlyUpdated(ContactService.getCacheFolder(), lastFinished);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Contact> getUpdatedContacts(final boolean includeHiddenProperties,
                                                     final long lastExecuted,
                                                     final long lastFinished
    ) {
        final PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        return getContacts(lastExecuted, propertyData, lastFinished);
    }

    public EngagementData getUpdatedEngagements(final Contact contact, long lastFinished) {
        final long contactId = contact.getId();
        if (lastFinished == -1) {
            lastFinished = FileUtils.findMostRecentModification(Paths.get("./cache/engagements/" + contactId));
        }
        try {
            return EngagementsProcessor.getUpdatedEngagements(httpService, contactId, lastFinished);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to get updated engagements for contact id {}", contactId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public HashMap<Long, Company> readCompanyJsons() {
        try {
            return CompanyService.readCompanyJsons();
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to read company jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, Contact> readContactJsons() {
        try {
            return ContactService.readContactJsons();
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to read contact jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public HashMap<Long, EngagementData> readEngagementJsons() {
        try {
            return EngagementsProcessor.readEngagementJsons();
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to read engagement jsons", e);
            System.exit(e.getCode());
            return new HashMap<>();
        }
    }

    public void writeCompanyJson(final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.COMPANIES, includeHiddenProperties);
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to write companies");
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeCompanyJson(final String propertyGroup, final boolean includeHiddenProperties) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.COMPANIES,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        try {
            CompanyService.writeCompanyJsons(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to write companies");
            FileUtils.deleteDirectory(CompanyService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactEngagementJsons(final long contactId) {
        try {
            EngagementsProcessor.writeContactEngagementJsons(httpService, contactId);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to write engagement for contact id {}", contactId, e);
            FileUtils.deleteDirectory(EngagementsProcessor.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactJson(final boolean includeHiddenProperties) {
        final PropertyData propertyData = allProperties(CRMObjectType.CONTACTS, includeHiddenProperties);
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to write contacts");
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }

    public void writeContactJson(final String propertyGroup, final boolean includeHiddenProperties) {
        final PropertyData propertyData = propsByGroupName(CRMObjectType.CONTACTS,
                                                           propertyGroup,
                                                           includeHiddenProperties
        );
        try {
            ContactService.writeContactJson(httpService, propertyData, rateLimiter);
        }
        catch (final HubSpotException e) {
            logger.fatal("Unable to write contacts");
            FileUtils.deleteDirectory(ContactService.getCacheFolder());
            System.exit(e.getCode());
        }
    }
}
