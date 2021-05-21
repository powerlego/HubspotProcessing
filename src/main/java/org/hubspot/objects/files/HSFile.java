package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

/**
 * A class to represent a Hubspot file
 *
 * @author Nicholas Curl
 */
public class HSFile extends HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger  logger           = LogManager.getLogger(HSFile.class);
    /**
     * The serial version UID for this class
     */
    private static final long    serialVersionUID = -8372406524250648086L;
    /**
     * The name of the file
     */
    private final        String  name;
    /**
     * The file extension of the file
     */
    private final        String  extension;
    /**
     * Is the file hidden
     */
    private final        boolean hidden;
    /**
     * The download url for this file
     */
    private final        String  url;
    /**
     * The engagement id that this file is associated with
     */
    private final        long    engagementId;
    /**
     * The size of the file in bytes
     */
    private final        long    size;

    /**
     * A constructor for the file
     *
     * @param id           The file id
     * @param engagementId The engagement id that the file is associated with
     * @param name         The name of the file
     * @param extension    The file extension of the file
     * @param size         The size of the file in bytes
     * @param hidden       Is the file hidden
     * @param url          The download url of the file
     */
    public HSFile(long id,
                  long engagementId,
                  String name,
                  String extension,
                  long size,
                  boolean hidden,
                  String url
    ) {
        super(id);
        this.name = name;
        this.extension = extension;
        this.size = size;
        this.hidden = hidden;
        this.url = url;
        this.engagementId = engagementId;
    }

    /**
     * Get the engagement id that this file is associated with
     *
     * @return The engagement id that this file is associated with
     */
    public long getEngagementId() {
        return engagementId;
    }

    /**
     * Gets the extension of this file
     *
     * @return The extension of this file
     */
    public String getExtension() {
        return extension;
    }

    /**
     * Gets the name of this file
     *
     * @return The name of this file
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the size of this file
     *
     * @return The size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * Gets the download url of this file
     *
     * @return The download url of this file
     */
    public String getUrl() {
        return url;
    }

    /**
     * Is this file hidden
     *
     * @return {@code true} if this file is hidden<br>
     *         {@code false} if this file is not hidden
     */
    public boolean isHidden() {
        return hidden;
    }

    /**
     * Returns the string representation of this file
     *
     * @return The string representation of this file
     */
    @Override
    public String toString() {
        return name + "." + extension;
    }
}
