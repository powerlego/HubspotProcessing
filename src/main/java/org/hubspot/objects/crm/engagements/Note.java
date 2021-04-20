package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.files.HSFile;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Note extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger       logger = LogManager.getLogger();
    private final        String       note;
    private final        List<Long>   attachmentIds;
    private              List<HSFile> attachments;

    public Note(long id, String note, List<Long> attachments) {
        super(id);
        this.note = note;
        this.attachmentIds = attachments;
        this.attachments = new ArrayList<>();
    }

    public String getNote() {
        return note;
    }

    public void addAttachment(HSFile attachment) {
        this.attachments.add(attachment);
    }

    public List<Long> getAttachmentIds() {
        return attachmentIds;
    }

    public List<HSFile> getAttachments() {
        return attachments;
    }

    public void setAttachments(List<HSFile> attachments) {
        this.attachments = attachments;
    }

    @Override
    public String toString() {
        return note + "\nHas Attachments: " + hasAttachments() + "\nAttachments: " + attachmentIds;
    }

    public boolean hasAttachments() {
        return !attachmentIds.isEmpty();
    }
}
