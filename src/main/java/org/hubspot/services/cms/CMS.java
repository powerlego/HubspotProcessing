package org.hubspot.services.cms;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.Engagement;
import org.hubspot.objects.crm.engagements.Note;
import org.hubspot.objects.files.HSFile;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.LogMarkers;
import org.hubspot.utils.exceptions.HubSpotException;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class CMS implements Serializable {

    /**
     * The instance of the logger
     */
    private static final Logger      logger           = LogManager.getLogger();
    /**
     * The serial version UID for this class
     */
    private static final long        serialVersionUID = -6357323675829216957L;
    private final        HttpService httpService;
    private final        RateLimiter rateLimiter;

    public CMS(HttpService httpService, final RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
        this.httpService = httpService;
    }

    public void downloadFile(Path folder, HSFile file) {
        try {
            FileService.downloadFile(folder, file, httpService, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to download file {}", file.toString(), e);
            System.exit(e.getCode());
        }
    }

    public void downloadFiles(Path folder, List<HSFile> files) {
        try {
            FileService.downloadFiles(folder, files, httpService, rateLimiter);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to Download Files", e);
            System.exit(e.getCode());
        }
    }

    public void getAllNoteAttachments(long contactId, List<Engagement> engagements) {
        FileService.getAllNoteAttachments(httpService, rateLimiter, contactId, engagements);
    }

    public HSFile getFile(long engagementId, long fileId) {
        try {
            return FileService.getFileMetadata(httpService, rateLimiter, engagementId, fileId);
        }
        catch (HubSpotException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to get File id {}", fileId, e);
            System.exit(e.getCode());
            return null;
        }
    }

    public List<HSFile> getFileMetadatas(Note note) {
        return FileService.getFileMetadatas(httpService, rateLimiter, note);
    }
}
