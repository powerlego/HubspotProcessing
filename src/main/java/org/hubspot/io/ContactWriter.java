package org.hubspot.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.ErrorCodes;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Nicholas Curl
 */
public class ContactWriter {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final Path contactsFolder = Paths.get("./contacts/contacts/");

    public static void write(Contact contact) {
        try {
            Files.createDirectories(contactsFolder);
        } catch (IOException e) {
            logger.fatal("Unable to create engagements folder", e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        Path filePath;
        String firstName = contact.getFirstName();
        String lastName = contact.getLastName();
        String email = contact.getEmail();
        boolean bFirstName = firstName == null || firstName.contains("null") || firstName.equalsIgnoreCase("N/A");
        boolean bLastName = lastName == null || lastName.contains("null") || lastName.equalsIgnoreCase("N/A");
        boolean bEmail = email == null || email.contains("null");
        if (bFirstName) {
            if (bLastName) {
                if (bEmail) {
                    filePath = contactsFolder.resolve(contact.getId() + ".txt");
                } else {
                    filePath = contactsFolder.resolve(email + "_" + contact.getId() + ".txt");
                }
            } else {
                lastName = lastName.replaceAll("\\s", "_");
                filePath = contactsFolder.resolve(lastName + "_" + contact.getId() + ".txt");
            }
        } else {
            firstName = firstName.replaceAll("\\s", "_");
            if (bLastName) {
                filePath = contactsFolder.resolve(firstName + "_" + contact.getId() + ".txt");
            } else {
                lastName = lastName.replaceAll("\\s", "_");
                filePath = contactsFolder.resolve(firstName + "_" + lastName + "_" + contact.getId() + ".txt");
            }
        }
        try {
            FileWriter writer = new FileWriter(filePath.toFile());
            writer.write(contact.toString());
            writer.close();
        } catch (IOException e) {
            logger.fatal("Unable to write to file", e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
