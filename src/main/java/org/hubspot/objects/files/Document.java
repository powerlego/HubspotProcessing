package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Nicholas Curl
 */
public class Document extends HSFile {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public Document(long id, long engagementId, String name, String extension, long size, boolean hidden, String url) {
        super(id, engagementId, name, extension, size, hidden, url);
    }

}
