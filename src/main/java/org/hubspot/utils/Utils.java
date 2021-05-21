package org.hubspot.utils;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Nicholas Curl
 */
public class Utils {

    /**
     * The instance of the logger
     */
    private static final Logger                   logger    = LogManager.getLogger(Utils.class);
    private static final List<ThreadPoolExecutor> executors = new ArrayList<>();

    public static void addExecutor(ThreadPoolExecutor executor) {
        executors.add(executor);
    }

    public static List<ThreadPoolExecutor> getExecutors() {
        return executors;
    }

    public static void adjustLoad(ThreadPoolExecutor threadPoolExecutor,
                                  double load,
                                  String debugMessage,
                                  Logger logger,
                                  int maxSize
    ) {
        logger.trace(LogMarkers.CPU_LOAD.getMarker(), debugMessage);
        int comparison = Double.compare(load, 50.0);
        if (comparison > 0 && threadPoolExecutor.getMaximumPoolSize() != threadPoolExecutor.getCorePoolSize()) {
            int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
            threadPoolExecutor.setMaximumPoolSize(numThreads);
        }
        else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != maxSize) {
            int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
            threadPoolExecutor.setMaximumPoolSize(numThreads);
        }
    }

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

    public static ProgressBar createProgressBar(String taskName) {
        return getProgressBarBuilder(taskName).build();
    }

    public static ProgressBarBuilder getProgressBarBuilder(String taskName) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(1)
                                       .setMaxRenderedLength(120);
    }

    public static ProgressBar createProgressBar(String taskName, long size) {
        return getProgressBarBuilder(taskName, size).build();
    }

    public static ProgressBarBuilder getProgressBarBuilder(String taskName, long size) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setInitialMax(size)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(1)
                                       .setMaxRenderedLength(120);
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

    public static JSONObject formatJson(JSONObject jsonObjectToFormat) {
        return (JSONObject) recurseCheckingFormat(jsonObjectToFormat);
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

    public static float round(float value, int places) {
        return (float) round((double) value, places);
    }

    public static double round(double value, int places) {
        if (places < 0) {
            throw new IllegalArgumentException();
        }
        BigDecimal bd = BigDecimal.valueOf(value).setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static void shutdownExecutors(Logger logger, ThreadPoolExecutor executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn(LogMarkers.ERROR.getMarker(), "Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executors.remove(executorService);
    }

    public static void shutdownUpdaters(Logger logger, ScheduledExecutorService scheduledExecutorService) {
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn(LogMarkers.ERROR.getMarker(), "Termination Timeout");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
