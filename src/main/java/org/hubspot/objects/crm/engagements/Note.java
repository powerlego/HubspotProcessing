package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.files.HSFile;

import java.util.ArrayList;
import java.util.List;

/**
 * A class representing a note engagement
 *
 * @author Nicholas Curl
 */
public class Note extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger       logger           = LogManager.getLogger(Note.class);
    /**
     * The serial version UID for this class
     */
    private static final long         serialVersionUID = 5193737134720012436L;
    /**
     * The body of the note
     */
    private final        String       note;
    /**
     * The list of attachment ids associated with this note
     */
    private final        List<Long>   attachmentIds;
    /**
     * The list of attachments associated with this note
     */
    private              List<HSFile> attachments;

    /**
     * A constructor for a Note object
     *
     * @param id            The engagement id
     * @param note          The body of the note
     * @param attachmentIds The list of attachment ids associated with this note
     */
    public Note(long id, String note, List<Long> attachmentIds) {
        super(id, EngagementType.NOTE);
        this.note = note;
        this.attachmentIds = attachmentIds;
        this.attachments = new ArrayList<>();
    }

    /**
     * Associates an attachment to this note
     *
     * @param attachment The attachment to associate
     */
    public void addAttachment(HSFile attachment) {
        this.attachments.add(attachment);
    }

    /**
     * Gets the list of attachment ids associated with this note
     *
     * @return The list of attachment ids associated with this note
     */
    public List<Long> getAttachmentIds() {
        return attachmentIds;
    }

    /**
     * Gets the list of attachments associated with this note
     *
     * @return The list of attachments associated with this note
     */
    public List<HSFile> getAttachments() {
        return attachments;
    }

    /**
     * Sets a list of attachments to be associated with this note
     *
     * @param attachments The list of attachments to associate with this note
     */
    public void setAttachments(List<HSFile> attachments) {
        this.attachments = attachments;
    }

    /**
     * Gets the body of the note
     *
     * @return The body of the note
     */
    public String getNote() {
        return note;
    }

    /**
     * Returns the string representation of this note
     *
     * @return The string representation of this note
     */
    @Override
    public String toString() {
        return note + "\nHas Attachments: " + hasAttachments() + "\nAttachments: " + attachmentIds;
    }

    /**
     * Does the note have associated attachments
     *
     * @return {@code true} if there are attachments associated with this note<br>
     *         {@code false} if there aren't any attachments associated with this note
     */
    public boolean hasAttachments() {
        return !attachmentIds.isEmpty();
    }
}
