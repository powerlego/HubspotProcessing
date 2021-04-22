package org.hubspot.objects.crm;

/**
 * An Enum of CRM object types
 *
 * @author Nicholas Curl
 */
public enum CRMObjectType {
    /**
     * CRM Contact object type
     */
    CONTACTS("contacts"),
    /**
     * CRM Company object type
     */
    COMPANIES("companies"),
    /**
     * CRM Deal object type
     */
    DEALS("deals"),
    /**
     * CRM Ticket object type
     */
    TICKETS("tickets");
    /**
     * The string value of the Enum constant
     */
    private final String value;

    /**
     * The constructor of the Enum constants
     *
     * @param value The string value of the Enum constant
     */
    CRMObjectType(String value) {
        this.value = value;
    }

    /**
     * Gets the string value of the Enum constant
     *
     * @return The string value of the Enum constant
     */
    public String getValue() {
        return value;
    }
}
