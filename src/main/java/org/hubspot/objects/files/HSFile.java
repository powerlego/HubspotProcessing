package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.HubSpotObject;

/**
 * @author Nicholas Curl
 */
public class HSFile extends HubSpotObject {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final String name;

    private final String  extension;
    private final boolean hidden;
    private final String  url;
    private final long    engagementId;
    private       long    size;

    public HSFile(long id, long engagementId, String name, String extension, long size, boolean hidden, String url) {
        super(id);
        this.name = name;
        this.extension = extension;
        this.size = size;
        this.hidden = hidden;
        this.url = url;
        this.engagementId = engagementId;
    }

    public long getEngagementId() {
        return engagementId;
    }

    public String getExtension() {
        return extension;
    }

    public String getName() {
        return name;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getUrl() {
        return url;
    }

    public boolean isHidden() {
        return hidden;
    }

    @Override
    public String toString() {
        return name + "." + extension;
    }
}
