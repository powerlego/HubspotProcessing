package org.hubspot.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Contact;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.LogMarkers;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Writes the processed Hubspot Contact to a file
 *
 * @author Nicholas Curl
 */
public class ContactWriter {

    /**
     * The instance of the logger
     */
    private static final Logger logger         = LogManager.getLogger();
    /**
     * The directory to write the processed contacts to
     */
    private static final Path   contactsFolder = Paths.get("./contacts/contacts/");

    /**
     * Writes the specified contact
     *
     * @param contact The contact to write
     */
    public static void write(Contact contact) {
        logger.traceEntry("contact={}", contact);
        //Tries to create the directory to store the written contacts
        try {
            Files.createDirectories(contactsFolder);
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to create engagements folder {}", contactsFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        Path filePath;
        String firstName = contact.getFirstName();
        String lastName = contact.getLastName();
        String email = contact.getEmail();
        boolean bFirstName = (firstName == null ||
                              firstName.contains("null") ||
                              firstName.equalsIgnoreCase("N/A")
        );
        boolean bLastName = (lastName == null || lastName.contains("null") || lastName.equalsIgnoreCase("N/A"));
        boolean bEmail = email == null || email.contains("null");
        /* Uses first and last name and the contact's id, uses the email address and the contact's id, or the contact's
           id for the filename
        */
        if (bFirstName) {
            if (bLastName) {
                if (bEmail) {
                    filePath = contactsFolder.resolve(contact.getId() + ".txt");
                }
                else {
                    filePath = contactsFolder.resolve(email + "_" + contact.getId() + ".txt");
                }
            }
            else {
                //Cleans up the last name
                lastName = lastName.replaceAll("\\s", "_");
                filePath = contactsFolder.resolve(lastName + "_" + contact.getId() + ".txt");
            }
        }
        else {
            //Cleans up the first name
            firstName = firstName.replaceAll("\\s", "_");
            if (bLastName) {
                filePath = contactsFolder.resolve(firstName + "_" + contact.getId() + ".txt");
            }
            else {
                //Cleans up the last name
                lastName = lastName.replaceAll("\\s", "_");
                filePath = contactsFolder.resolve(firstName + "_" + lastName + "_" + contact.getId() + ".txt");
            }
        }
        //Tries to write the file, but will exit if unable
        try {
            FileWriter writer = new FileWriter(filePath.toFile());
            writer.write(contact.toString());
            writer.close();
        }
        catch (IOException e) {
            if (e.getMessage().contains("(The filename, directory name, or volume label syntax is incorrect)")) {
                logger.debug(LogMarkers.CORRECTION.getMarker(), "Needs to be corrected: {}", contact);
            }
            else {
                logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write to file {}", filePath, e);
                System.exit(ErrorCodes.IO_WRITE.getErrorCode());
            }
        }
    }
}
