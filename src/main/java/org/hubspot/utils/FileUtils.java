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
    private static final Logger logger = LogManager.getLogger(FileUtils.class);

    /**
     * Deletes the specified directory @param directoryToBeDeleted The directory to be deleted
     */
    public static void deleteDirectory(Path directoryToBeDeleted) {
        try {
            Files.walkFileTree(directoryToBeDeleted, new DeletingVisitor(false));
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to delete directory {}", directoryToBeDeleted, e);
        }
    }

    public static void deleteRecentlyUpdated(File folder, long lastFinished) {
        File[] files = folder.listFiles(pathname -> pathname.lastModified() > lastFinished);
        if (files != null && files.length > 0) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteRecentlyUpdated(file, lastFinished);
                }
                else {
                    try {
                        Files.delete(file.toPath());
                    }
                    catch (IOException e) {
                        logger.fatal(LogMarkers.ERROR.getMarker(),
                                     "Unable to delete file {}. Please delete manually.",
                                     file,
                                     e
                        );
                    }
                }
            }
        }
    }

    public static void deleteRecentlyUpdated(Path folder, long lastFinished) {
        deleteRecentlyUpdated(folder.toFile(), lastFinished);
    }

    public static void downloadFile(String downloadUrlString, Path folder, HSFile file) throws HubSpotException {
        try {
            URL downloadURL = new URL(downloadUrlString);
            File dest = folder.resolve(file.getName() + "." + file.getExtension()).toFile();
            try {
                org.apache.commons.io.FileUtils.copyURLToFile(downloadURL, dest);
            }
            catch (IOException e) {
                throw new HubSpotException("Unable to download file " + file + " Id " + file.getId(),
                                           ErrorCodes.IO_DOWNLOAD.getErrorCode(),
                                           e
                );
            }
        }
        catch (MalformedURLException e) {
            throw new HubSpotException("Malformed URL", ErrorCodes.MALFORMED_URL.getErrorCode(), e);
        }
    }

    public static long findMostRecentModification(Path dir) {
        return findMostRecentModification(dir.toFile());
    }

    public static long findMostRecentModification(File dir) {
        long lastModified = -1;
        if (dir.isDirectory()) {
            File[] files = dir.listFiles(File::isFile);
            if (files != null && files.length > 0) {
                Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
                lastModified = files[0].lastModified();
            }
        }
        return lastModified;
    }

    public static String readJsonString(Logger logger, Path path) {
        return readJsonString(logger, path.toFile());
    }

    public static String readJsonString(Logger logger, File file) {
        String jsonString = "";
        try {
            jsonString = readFile(file);
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to read json string", e);
            System.exit(ErrorCodes.IO_READ.getErrorCode());
        }
        return jsonString;
    }

    public static String readFile(File file) throws IOException {
        String fileString;
        FileInputStream inputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        StringBuilder fileStringBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            line = line.strip();
            fileStringBuilder.append(line).append("\n");
        }
        fileString = fileStringBuilder.toString().strip();
        return fileString;
    }

    public static long readLastExecution() {
        long lastExecuted = -1;
        Path lastExecutedFile = Paths.get("./cache/last_executed.txt");
        try {
            String value = readFile(lastExecutedFile);
            lastExecuted = Long.parseLong(value);
        }
        catch (NumberFormatException | IOException ignored) {
        }
        return lastExecuted;
    }

    public static String readFile(Path path) throws IOException {
        return readFile(path.toFile());
    }

    public static long readLastFinished() {
        long lastFinished = -1;
        Path lastFinishedFile = Paths.get("./cache/last_finished.txt");
        try {
            String value = readFile(lastFinishedFile);
            lastFinished = Long.parseLong(value);
        }
        catch (NumberFormatException | IOException ignored) {
        }
        return lastFinished;
    }

    public static void writeFile(Path path, Object o) throws IOException {
        writeFile(path.toFile(), o);
    }

    public static void writeFile(File file, Object o) throws IOException {
        FileWriter writer = new FileWriter(file);
        writer.write(o.toString());
        writer.close();
    }

    public static void writeJsonCache(Path folder, JSONObject jsonObject) throws IOException {
        Path filePath = getFilePath(folder, jsonObject);
        FileWriter fileWriter = new FileWriter(filePath.toFile());
        fileWriter.write(jsonObject.toString(4));
        fileWriter.close();
    }

    private static Path getFilePath(Path folder, JSONObject jsonObject) {
        long id;
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
        long lastExecuted = Instant.now().toEpochMilli();
        Path lastExecutedFile = Paths.get("./cache/last_executed.txt");
        try {
            FileWriter fileWriter = new FileWriter(lastExecutedFile.toFile());
            fileWriter.write(String.valueOf(lastExecuted));
            fileWriter.close();
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write file {}", lastExecutedFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
        return lastExecuted;
    }

    public static void writeLastFinished() {
        long lastFinished = Instant.now().toEpochMilli();
        Path lastFinishedFile = Paths.get("./cache/last_finished.txt");
        try {
            FileWriter fileWriter = new FileWriter(lastFinishedFile.toFile());
            fileWriter.write(String.valueOf(lastFinished));
            fileWriter.close();
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to write file {}", lastFinishedFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
