package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class to represent an Other file
 *
 * @author Nicholas Curl
 */
public class Other extends HSFile {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(Other.class);
    private static final long   serialVersionUID = -4885872607922692974L;

    public Other(long id,
                 long engagementId,
                 String name,
                 String extension,
                 long size,
                 boolean hidden,
                 String url
    ) {
        super(id, engagementId, name, extension, size, hidden, url);
    }
}
