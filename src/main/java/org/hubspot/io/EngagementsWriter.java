package org.hubspot.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.engagements.*;
import org.hubspot.utils.ErrorCodes;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class EngagementsWriter {

    /**
     * The instance of the logger
     */
    private static final Logger logger            = LogManager.getLogger();
    private static final Path   engagementsFolder = Paths.get("./contacts/engagements/");

    public static void write(long id, List<?> engagements) {
        try {
            Files.createDirectories(engagementsFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create engagements folder {}", engagementsFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        Path contact = engagementsFolder.resolve(id + "/");
        try {
            Files.createDirectories(contact);
        }
        catch (IOException e) {
            logger.fatal("Unable to create contact directory for id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        Path emails = contact.resolve("emails/");
        Path notes = contact.resolve("notes/");
        Path tasks = contact.resolve("tasks/");
        Path calls = contact.resolve("calls/");
        Path meetings = contact.resolve("meetings/");
        try {
            Files.createDirectories(emails);
        }
        catch (IOException e) {
            logger.fatal("Unable to make emails directory for contact id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try {
            Files.createDirectories(notes);
        }
        catch (IOException e) {
            logger.fatal("Unable to make notes directory for contact id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try {
            Files.createDirectories(tasks);
        }
        catch (IOException e) {
            logger.fatal("Unable to make tasks directory for contact id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try {
            Files.createDirectories(calls);
        }
        catch (IOException e) {
            logger.fatal("Unable to make calls directory for contact id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try {
            Files.createDirectories(meetings);
        }
        catch (IOException e) {
            logger.fatal("Unable to make meetings directory for contact id {}", id, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        int emailNum = 0;
        int noteNum = 0;
        int meetingNum = 0;
        int callNum = 0;
        int taskNum = 0;
        for (Object o : engagements) {
            if (o instanceof Email) {
                Email email = (Email) o;
                Path emailPath = emails.resolve("email_" + emailNum + ".txt");
                try {
                    FileWriter writer = new FileWriter(emailPath.toFile());
                    writer.write(email.toString());
                    writer.close();
                    emailNum++;
                }
                catch (IOException e) {
                    logger.fatal("Unable to write to file {}", emailPath, e);
                    System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                }
            }
            else if (o instanceof Note) {
                Note note = (Note) o;
                Path notePath = emails.resolve("note_" + noteNum + ".txt");
                try {
                    FileWriter writer = new FileWriter(notePath.toFile());
                    writer.write(note.toString());
                    writer.close();
                    noteNum++;
                }
                catch (IOException e) {
                    logger.fatal("Unable to write to file {}", notePath, e);
                    System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                }
            }
            else if (o instanceof Meeting) {
                Meeting meeting = (Meeting) o;
                Path meetingPath = emails.resolve("meeting_" + meetingNum + ".txt");
                try {
                    FileWriter writer = new FileWriter(meetingPath.toFile());
                    writer.write(meeting.toString());
                    writer.close();
                    meetingNum++;
                }
                catch (IOException e) {
                    logger.fatal("Unable to write to file {}", meetingPath, e);
                    System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                }
            }
            else if (o instanceof Call) {
                Call call = (Call) o;
                Path callPath = calls.resolve("call_" + callNum + ".txt");
                try {
                    FileWriter writer = new FileWriter(callPath.toFile());
                    writer.write(call.toString());
                    writer.close();
                    callNum++;
                }
                catch (IOException e) {
                    logger.fatal("Unable to write to file {}", callPath, e);
                    System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                }
            }
            else if (o instanceof Task) {
                Task task = (Task) o;
                Path taskPath = tasks.resolve("task_" + taskNum + ".txt");
                try {
                    FileWriter writer = new FileWriter(taskPath.toFile());
                    writer.write(task.toString());
                    writer.close();
                    taskNum++;
                }
                catch (IOException e) {
                    logger.fatal("Unable to write to file {}", taskPath, e);
                    System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                }
            }
            else {
                logger.fatal("Unknown engagement {}", o);
                System.exit(ErrorCodes.INVALID_ENGAGEMENT.getErrorCode());
            }
        }
    }
}
