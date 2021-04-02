package org.hubspot.objects.crm;

/**
 * @author Nicholas Curl
 */
public enum CRMObjectType {
    CONTACTS("CONTACTS"),
    COMPANIES("COMPANIES"),
    DEALS("DEALS"),
    TICKETS("TICKETS");

    private final String value;

    CRMObjectType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
