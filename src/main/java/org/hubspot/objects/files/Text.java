package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class to represent a Text file
 *
 * @author Nicholas Curl
 */
public class Text extends HSFile {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger();
    private static final long   serialVersionUID = 5126768163531293252L;

    public Text(long id,
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
