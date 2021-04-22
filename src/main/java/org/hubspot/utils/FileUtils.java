package org.hubspot.utils;

import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.files.HSFile;
import org.hubspot.utils.exceptions.HubSpotException;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;

/**
 * @author Nicholas Curl
 */
public class FileUtils {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Deletes the specified directory @param directoryToBeDeleted The directory to be deleted
     */
    public static void deleteDirectory(final Path directoryToBeDeleted) {
        try {
            Files.walkFileTree(directoryToBeDeleted, new DeletingVisitor(false));
        }
        catch (final IOException e) {
            logger.fatal("Unable to delete directory {}", directoryToBeDeleted, e);
        }
    }

    public static void deleteRecentlyUpdated(final File folder, final long lastFinished) {
        final File[] files = folder.listFiles(pathname -> pathname.lastModified() > lastFinished);
        if (files != null && files.length > 0) {
            for (final File file : files) {
                if (file.isDirectory()) {
                    deleteRecentlyUpdated(file, lastFinished);
                }
                else {
                    try {
                        Files.delete(file.toPath());
                    }
                    catch (final IOException e) {
                        logger.fatal("Unable to delete file {}. Please delete manually.", file, e);
                    }
                }
            }
        }
    }

    public static void deleteRecentlyUpdated(final Path folder, final long lastFinished) {
        deleteRecentlyUpdated(folder.toFile(), lastFinished);
    }

    public static void downloadFile(final String downloadUrlString, final Path folder, final HSFile file)
    throws HubSpotException {
        try {
            final URL downloadURL = new URL(downloadUrlString);
            final File dest = folder.resolve(file.getName() + "." + file.getExtension()).toFile();
            try {
                org.apache.commons.io.FileUtils.copyURLToFile(downloadURL, dest);
            }
            catch (final IOException e) {
                throw new HubSpotException("Unable to download file " + file + " Id " + file.getId(),
                                           ErrorCodes.IO_DOWNLOAD.getErrorCode(),
                                           e
                );
            }
        }
        catch (final MalformedURLException e) {
            throw new HubSpotException("Malformed URL", ErrorCodes.MALFORMED_URL.getErrorCode(), e);
        }
    }

    public static long findMostRecentModification(final Path dir) {
        return findMostRecentModification(dir.toFile());
    }

    public static long findMostRecentModification(final File dir) {
        long lastModified = -1;
        if (dir.isDirectory()) {
            final File[] files = dir.listFiles(File::isFile);
            if (files != null && files.length > 0) {
                Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
                lastModified = files[0].lastModified();
            }
        }
        return lastModified;
    }

    public static String readJsonString(final Logger logger, final Path path) {
        return readJsonString(logger, path.toFile());
    }

    public static String readJsonString(final Logger logger, final File file) {
        String jsonString = "";
        try {
            jsonString = readFile(file);
        }
        catch (final IOException e) {
            logger.fatal("Unable to read json string", e);
            System.exit(ErrorCodes.IO_READ.getErrorCode());
        }
        return jsonString;
    }

    public static String readFile(final File file) throws IOException {
        final String fileString;
        final FileInputStream inputStream = new FileInputStream(file);
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        final StringBuilder fileStringBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            line = line.strip();
            fileStringBuilder.append(line).append("\n");
        }
        fileString = fileStringBuilder.toString().strip();
        return fileString;
    }

    public static long readLastExecution() {
        long lastExecuted = -1;
        final Path lastExecutedFile = Paths.get("./cache/last_executed.txt");
        try {
            final String value = readFile(lastExecutedFile);
            lastExecuted = Long.parseLong(value);
        }
        catch (final NumberFormatException | IOException ignored) {
        }
        return lastExecuted;
    }

    public static String readFile(final Path path) throws IOException {
        return readFile(path.toFile());
    }

    public static long readLastFinished() {
        long lastFinished = -1;
        final Path lastFinishedFile = Paths.get("./cache/last_finished.txt");
        try {
            final String value = readFile(lastFinishedFile);
            lastFinished = Long.parseLong(value);
        }
        catch (final NumberFormatException | IOException ignored) {
        }
        return lastFinished;
    }

    public static void writeFile(final Path path, final Object o) throws IOException {
        writeFile(path.toFile(), o);
    }

    public static void writeFile(final File file, final Object o) throws IOException {
        final FileWriter writer = new FileWriter(file);
        writer.write(o.toString());
        writer.close();
    }

    public static void writeJsonCache(final Path folder, final JSONObject jsonObject) throws IOException {
        final Path filePath = getFilePath(folder, jsonObject);
        final FileWriter fileWriter = new FileWriter(filePath.toFile());
        fileWriter.write(jsonObject.toString(4));
        fileWriter.close();
    }

    private static Path getFilePath(final Path folder, final JSONObject jsonObject) {
        final long id;
        if (jsonObject.has("id")) {
            id = jsonObject.getLong("id");
        }
        else if (jsonObject.has("engagement")) {
            id = jsonObject.getJSONObject("engagement").getLong("id");
        }
        else {
            id = 0;
        }
        return folder.resolve(id + ".json");
    }

    public static long writeLastExecution() {
        final long lastExecuted = Instant.now().toEpochMilli();
        final Path lastExecutedFile = Paths.get("./cache/last_executed.txt");
        try {
            final FileWriter fileWriter = new FileWriter(lastExecutedFile.toFile());
            fileWriter.write(String.valueOf(lastExecuted));
            fileWriter.close();
        }
        catch (final IOException e) {
            logger.fatal("Unable to write file {}", lastExecutedFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
        return lastExecuted;
    }

    public static void writeLastFinished() {
        final long lastFinished = Instant.now().toEpochMilli();
        final Path lastFinishedFile = Paths.get("./cache/last_finished.txt");
        try {
            final FileWriter fileWriter = new FileWriter(lastFinishedFile.toFile());
            fileWriter.write(String.valueOf(lastFinished));
            fileWriter.close();
        }
        catch (final IOException e) {
            logger.fatal("Unable to write file {}", lastFinishedFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
