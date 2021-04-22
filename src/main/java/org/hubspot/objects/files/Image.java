package org.hubspot.objects.files;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class to represent an Image file
 *
 * @author Nicholas Curl
 */
public class Image extends HSFile {

    /**
     * The instance of the logger
     */
    private static final Logger logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long   serialVersionUID = 8674458467181889203L;
    /**
     * The width of the image in pixels
     */
    private final        long   width;
    /**
     * The height of the image in pixels
     */
    private final        long   height;

    /**
     * A constructor for an image file
     *
     * @param id           The file id of the image
     * @param engagementId The engagement id that the image is associated with
     * @param name         The name of the image
     * @param extension    The file extension of the image
     * @param size         The size of the image in bytes
     * @param hidden       Is the image hidden
     * @param url          The download url of the image
     * @param width        The width of the image in pixels
     * @param height       The height of the image in pixels
     */
    public Image(final long id,
                 final long engagementId,
                 final String name,
                 final String extension,
                 final long size,
                 final boolean hidden,
                 final String url,
                 final long width,
                 final long height
    ) {
        super(id, engagementId, name, extension, size, hidden, url);
        this.width = width;
        this.height = height;
    }

    /**
     * Gets the height of this image in pixels
     *
     * @return The height of this image in pixels
     */
    public long getHeight() {
        return height;
    }

    /**
     * Gets the width of this image in pixels
     *
     * @return The width of this image in pixels
     */
    public long getWidth() {
        return width;
    }
}
