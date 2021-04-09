package org.hubspot.objects.crm;

/**
 * @author Nicholas Curl
 */
public enum CRMObjectType {
    CONTACTS("contacts"), COMPANIES("companies"), DEALS("deals"), TICKETS("tickets");
    private final String value;

    CRMObjectType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
