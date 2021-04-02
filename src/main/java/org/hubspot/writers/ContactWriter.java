package org.hubspot.writers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.Contact;

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
            System.exit(-1);
        }
        Path filePath;
        if (contact.getFirstname() == null) {
            filePath = contactsFolder.resolve(contact.getLastname() + "_" + contact.getId() + ".txt");
        } else if (contact.getFirstname().equalsIgnoreCase("N/A")) {
            filePath = contactsFolder.resolve(contact.getLastname() + "_" + contact.getId() + ".txt");
        } else if (contact.getLastname() == null) {
            filePath = contactsFolder.resolve(contact.getFirstname() + "_" + contact.getId() + ".txt");
        } else if (contact.getLastname().equalsIgnoreCase("N/A")) {
            filePath = contactsFolder.resolve(contact.getFirstname() + "_" + contact.getId() + ".txt");
        } else {
            filePath = contactsFolder.resolve(contact.getFirstname() + "_" + contact.getLastname() + "_" + contact.getId() + ".txt");
        }
        try {
            FileWriter writer = new FileWriter(filePath.toFile());
            writer.write(contact.toString());
            writer.close();
        } catch (IOException e) {
            logger.fatal("Unable to write to file", e);
            System.exit(-1);
        }
    }
}
