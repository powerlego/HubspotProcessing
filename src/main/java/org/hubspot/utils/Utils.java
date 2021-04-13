package org.hubspot.utils;


import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Nicholas Curl
 */
public class Utils {

    public static final  Instant now    = Instant.now();
    /**
     * The instance of the logger
     */
    private static final Logger  logger = LogManager.getLogger();

    public static <T> T convertInstanceObject(Object o, Class<T> clazz) {
        try {
            return clazz.cast(o);
        }
        catch (ClassCastException e) {
            return null;
        }
    }

    public static Object convertType(kong.unirest.json.JSONObject jsonToConvert) {
        Set<String> keys = jsonToConvert.keySet();
        JSONObject jsonObject = new JSONObject();
        for (String key : keys) {
            Object o = jsonToConvert.get(key);
            jsonObject.put(key, recurseCheckingConversion(o));
        }
        return jsonObject;
    }

    public static String createLineDivider(int length) {
        return "\n" + "-".repeat(Math.max(0, length)) + "\n";
    }

    public static ProgressBar createProgressBar(String taskName, long size) {
        return getProgressBarBuilder(taskName, size).build();
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
                        logger.fatal("Unable to delete file {}. Please delete manually.", file, e);
                    }
                }
            }
        }
    }

    public static ProgressBar createProgressBar(String taskName) {
        return getProgressBarBuilder(taskName).build();
    }

    public static void deleteRecentlyUpdated(Path folder, long lastFinished) {
        deleteRecentlyUpdated(folder.toFile(), lastFinished);
    }

    public static String format(String string) {
        return format(string, 80);
    }

    public static String format(String string, int columnWrap) {
        Pattern paragraphs = Pattern.compile("<p>(.*?)</p>");
        Matcher paragraphMatcher = paragraphs.matcher(string);
        StringBuilder stringBuilder = new StringBuilder();
        while (paragraphMatcher.find()) {
            String s = paragraphMatcher.group(1);
            stringBuilder.append(s).append("\n");
        }
        string = stringBuilder.toString().strip();
        Pattern anchors = Pattern.compile("<a.*?>(.*?)</a>");
        Matcher anchorMatcher = anchors.matcher(string);
        while (anchorMatcher.find()) {
            string = string.replace(anchorMatcher.group(0), anchorMatcher.group(1));
        }
        String[] noteSplit = string.split("\n");
        stringBuilder = new StringBuilder();
        for (String s : noteSplit) {
            String line = WordUtils.wrap(s, columnWrap, "\n", false);
            stringBuilder.append(line).append("\n");
        }
        string = stringBuilder.toString().strip().replace("&amp;", "&");
        return string;
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

    public static JSONObject formatJson(JSONObject jsonObjectToFormat) {
        return (JSONObject) recurseCheckingFormat(jsonObjectToFormat);
    }

    public static long getObjectCount(HttpService service, CRMObjectType type, RateLimiter rateLimiter) {
        JSONObject body = new JSONObject();
        JSONArray filterGroupsArray = new JSONArray();
        JSONObject filters = new JSONObject();
        JSONArray filtersArray = new JSONArray();
        JSONObject filter = new JSONObject();
        JSONArray propertyArray = new JSONArray(PropertyData.EMPTY.getPropertyNames());
        filter.put("propertyName", "hs_object_id").put("operator", "HAS_PROPERTY");
        filtersArray.put(filter);
        filters.put("filters", filtersArray);
        filterGroupsArray.put(filters);
        body.put("filterGroups", filterGroupsArray);
        body.put("properties", propertyArray).put("limit", 1);
        try {
            rateLimiter.acquire();
            JSONObject resp = (JSONObject) service.postRequest("/crm/v3/objects/" + type.getValue() + "/search", body);
            return resp.getLong("total");
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get object count.", e);
            return 0;
        }
    }

    public static ProgressBarBuilder getProgressBarBuilder(String taskName, long size) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setInitialMax(size)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(5).setMaxRenderedLength(-1);
    }

    public static ProgressBarBuilder getProgressBarBuilder(String taskName) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(5).setMaxRenderedLength(-1);
    }


    public static String propertyListToString(List<String> properties) {
        return properties.toString().replace("[", "").replace("]", "").replace(" ", "");
    }

    public static String readJsonString(Logger logger, Path path) {
        return readJsonString(logger, path.toFile());
    }

    public static String readJsonString(Logger logger, File file) {
        String jsonString = "";
        try {
            jsonString = Utils.readFile(file);
        }
        catch (IOException e) {
            logger.fatal("Unable to read json string", e);
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

    public static long getUpdateCount(HttpService service,
                                      RateLimiter rateLimiter,
                                      CRMObjectType type,
                                      long lastExecution
    ) {
        JSONObject body = getUpdateBody(type, PropertyData.EMPTY, lastExecution, 1);
        try {
            rateLimiter.acquire();
            JSONObject resp = (JSONObject) service.postRequest("/crm/v3/objects/" + type.getValue() + "/search", body);
            return resp.getLong("total");
        }
        catch (HubSpotException e) {
            logger.fatal("Unable to get object count.", e);
            return 0;
        }
    }

    public static JSONObject getUpdateBody(CRMObjectType type,
                                           PropertyData propertyData,
                                           long lastExecution,
                                           int limit
    ) {
        JSONObject body = new JSONObject();
        JSONArray filterGroupsArray = new JSONArray();
        JSONObject filters1 = new JSONObject();
        JSONArray filtersArray1 = new JSONArray();
        JSONObject filter1 = new JSONObject();
        JSONObject filters2 = new JSONObject();
        JSONArray filtersArray2 = new JSONArray();
        JSONObject filter2 = new JSONObject();
        filter1.put("propertyName", "createdate").put("operator", "GTE").put("value", lastExecution);
        filtersArray1.put(filter1);
        filters1.put("filters", filtersArray1);
        switch (type) {
            case CONTACTS:
                filter2.put("propertyName", "lastmodifieddate").put("operator", "GTE").put("value", lastExecution);
                break;
            case COMPANIES:
            case DEALS:
            case TICKETS:
                filter2.put("propertyName", "hs_lastmodifieddate").put("operator", "GTE").put("value", lastExecution);
                break;
        }
        filtersArray2.put(filter2);
        filters2.put("filters", filtersArray2);
        filterGroupsArray.put(filters1).put(filters2);
        JSONArray propertyArray = new JSONArray(propertyData.getPropertyNames());
        body.put("filterGroups", filterGroupsArray);
        body.put("properties", propertyArray);
        body.put("limit", limit);
        return body;
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

    public static String readFile(Path path) throws IOException {
        return readFile(path.toFile());
    }

    private static Object recurseCheckingConversion(Object object) {
        if (object instanceof kong.unirest.json.JSONObject) {
            kong.unirest.json.JSONObject o = (kong.unirest.json.JSONObject) object;
            JSONObject jsonObject = new JSONObject();
            Set<String> keys = o.keySet();
            for (String key : keys) {
                Object o1 = o.get(key);
                jsonObject.put(key, recurseCheckingConversion(o1));
            }
            return jsonObject;
        }
        else if (object instanceof kong.unirest.json.JSONArray) {
            kong.unirest.json.JSONArray o = (kong.unirest.json.JSONArray) object;
            JSONArray jsonArray = new JSONArray();
            for (Object o1 : o) {
                jsonArray.put(recurseCheckingConversion(o1));
            }
            return jsonArray;
        }
        else if (object instanceof String) {
            String o = (String) object;
            try {
                return Long.parseLong(o);
            }
            catch (NumberFormatException e) {
                if (o.equalsIgnoreCase("true") || o.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(o);
                }
                else {
                    return o;
                }
            }
        }
        else {
            return Objects.requireNonNullElse(object, JSONObject.NULL);
        }
    }

    private static Object recurseCheckingFormat(Object object) {
        if (object instanceof JSONObject) {
            JSONObject o = (JSONObject) object;
            JSONObject jsonObject = new JSONObject();
            Set<String> keys = o.keySet();
            for (String key : keys) {
                Object o1 = o.get(key);
                jsonObject.put(key, recurseCheckingFormat(o1));
            }
            return jsonObject;
        }
        else if (object instanceof JSONArray) {
            JSONArray o = (JSONArray) object;
            JSONArray jsonArray = new JSONArray();
            for (Object o1 : o) {
                jsonArray.put(recurseCheckingFormat(o1));
            }
            return jsonArray;
        }
        else if (object instanceof String) {
            String o = (String) object;
            try {
                return Long.parseLong(o);
            }
            catch (NumberFormatException e) {
                if (o.equalsIgnoreCase("true") || o.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(o);
                }
                else {
                    return o;
                }
            }
        }
        else {
            return Objects.requireNonNullElse(object, JSONObject.NULL);
        }
    }

    public static void shutdownExecutors(Logger logger, ExecutorService executorService, Path folder) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            deleteDirectory(folder);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
    }

    public static void shutdownForkJoinPool(Logger logger, ForkJoinPool forkJoinPool, Path folder) {
        forkJoinPool.shutdown();
        try {
            if (!forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            deleteDirectory(folder);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
    }

    public static void shutdownUpdateExecutors(Logger logger,
                                               ExecutorService executorService,
                                               Path folder,
                                               long lastFinished
    ) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            Utils.deleteRecentlyUpdated(folder, lastFinished);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
    }

    /**
     * Deletes the specified directory @param directoryToBeDeleted The directory to be deleted
     */
    public static void deleteDirectory(Path directoryToBeDeleted) {
        try {
            Files.walkFileTree(directoryToBeDeleted, new DeletingVisitor(false));
        }
        catch (IOException e) {
            logger.fatal("Unable to delete directory {}", directoryToBeDeleted, e);
            System.exit(ErrorCodes.IO_DELETE_DIRECTORY.getErrorCode());
        }
    }

    public static void shutdownExecutors(Logger logger, ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
    }

    public static void shutdownUpdateForkJoinPool(Logger logger,
                                                  ForkJoinPool forkJoinPool,
                                                  Path folder,
                                                  long lastFinished
    ) {
        forkJoinPool.shutdown();
        try {
            if (!forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            logger.fatal("Thread interrupted", e);
            Utils.deleteRecentlyUpdated(folder, lastFinished);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
    }

    public static void sleep(int seconds) {
        sleep((long) 1000 * seconds);
    }

    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T[][] splitArray(T[] arrayToSplit, int chunkSize) {
        if (chunkSize <= 0) {
            return null;
        }
        int rest = arrayToSplit.length % chunkSize;
        int chunks = arrayToSplit.length / chunkSize + (rest > 0 ? 1 : 0);
        Class<?> arrayType = arrayToSplit.getClass().getComponentType();
        @SuppressWarnings("unchecked") T[][] arrays = (T[][]) Array.newInstance(arrayType, chunks, 0);
        for (int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++) {
            arrays[i] = Arrays.copyOfRange(arrayToSplit, i * chunkSize, i * chunkSize + chunkSize);
        }
        if (rest > 0) {
            arrays[chunks - 1] = Arrays.copyOfRange(arrayToSplit,
                                                    (chunks - 1) * chunkSize,
                                                    (chunks - 1) * chunkSize + rest
            );
        }
        return arrays;
    }

    public static <T> List<List<T>> splitList(List<T> listToSplit, int chunkSize) {
        if (chunkSize <= 0) {
            return null;
        }
        int rest = listToSplit.size() % chunkSize;
        int chunks = listToSplit.size() / chunkSize + (rest > 0 ? 1 : 0);
        List<List<T>> lists = new ArrayList<>(chunks);
        for (int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++) {
            ArrayList<T> sublist = new ArrayList<>(listToSplit.subList(i * chunkSize, i * chunkSize + chunkSize));
            lists.add(i, sublist);
        }
        if (rest > 0) {
            ArrayList<T> sublist = new ArrayList<>(listToSplit.subList((chunks - 1) * chunkSize,
                                                                       (chunks - 1) * chunkSize + rest
            ));
            lists.add(chunks - 1, sublist);
        }
        return lists;
    }

    public static void writeJsonCache(ForkJoinPool forkJoinPool, Path folder, JSONObject jsonObject) {
        Path cacheRoot = Paths.get("./cache/");
        Path filePath = getFilePath(folder, jsonObject);
        try {
            FileWriter fileWriter = new FileWriter(filePath.toFile());
            fileWriter.write(jsonObject.toString(4));
            fileWriter.close();
        }
        catch (IOException e) {
            logger.fatal("Unable to write file {}", cacheRoot.relativize(filePath), e);
            forkJoinPool.shutdownNow();
            long wait = 0;
            while (forkJoinPool.isTerminating()) {
                wait++;
            }
            Utils.deleteDirectory(cacheRoot.resolve(cacheRoot.relativize(filePath).getName(0)));
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
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

    public static void writeJsonCache(ExecutorService executorService, Path folder, JSONObject jsonObject) {
        Path cacheRoot = Paths.get("./cache/");
        Path filePath = getFilePath(folder, jsonObject);
        try {
            FileWriter fileWriter = new FileWriter(filePath.toFile());
            fileWriter.write(jsonObject.toString(4));
            fileWriter.close();
        }
        catch (IOException e) {
            logger.fatal("Unable to write file {}", cacheRoot.relativize(filePath), e);
            executorService.shutdownNow();
            try {
                if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    logger.warn("Termination Timeout");
                }
            }
            catch (InterruptedException interruptedException) {
                logger.fatal("Thread interrupted", interruptedException);
                System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
            }
            Utils.deleteDirectory(cacheRoot.resolve(cacheRoot.relativize(filePath).getName(0)));
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
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
            logger.fatal("Unable to write file {}", lastExecutedFile, e);
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
            logger.fatal("Unable to write file {}", lastFinishedFile, e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
