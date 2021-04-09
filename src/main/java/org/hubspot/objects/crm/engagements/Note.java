package org.hubspot.objects.crm.engagements;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * @author Nicholas Curl
 */
public class Note extends Engagement {

    /**
     * The instance of the logger
     */
    private static final Logger     logger = LogManager.getLogger();
    private final        String     note;
    private final        List<Long> attachments;

    public Note(long id, String note, List<Long> attachments) {
        super(id);
        this.note = note;
        this.attachments = attachments;
    }

    public String getNote() {
        return note;
    }

    @Override
    public String toString() {
        return note + "\nHas Attachments: " + hasAttachments() + "\nAttachments: " + attachments;
    }

    public boolean hasAttachments() {
        return !attachments.isEmpty();
    }
}
