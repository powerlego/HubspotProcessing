package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class to represent a Document file
 *
 * @author Nicholas Curl
 */
public class Document extends HSFile {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger(Document.class);
    /**
     * The serial version UID for this class
     */
    private static final long   serialVersionUID = -7777724710221628185L;

    /**
     * A constructor for a document file
     *
     * @param id           The file id
     * @param engagementId The engagement id that the document is associated to
     * @param name         The name of the document
     * @param extension    The file extension of the document
     * @param size         The size of the document in bytes
     * @param hidden       Is the document hidden
     * @param url          The url to download the document
     */
    public Document(long id,
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
