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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
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
    private static final Logger logger = LogManager.getLogger();

    public static void adjustLoad(final ThreadPoolExecutor threadPoolExecutor,
                                  final double load,
                                  final String debugMessage,
                                  final Logger logger,
                                  final int maxSize
    ) {
        logger.trace(debugMessage);
        final int comparison = Double.compare(load, 50.0);
        if (comparison > 0 && threadPoolExecutor.getMaximumPoolSize() != threadPoolExecutor.getCorePoolSize()) {
            final int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
            threadPoolExecutor.setMaximumPoolSize(numThreads);
        }
        else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != maxSize) {
            final int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
            threadPoolExecutor.setMaximumPoolSize(numThreads);
        }
    }

    public static <T> T convertInstanceObject(final Object o, final Class<T> clazz) {
        try {
            return clazz.cast(o);
        }
        catch (final ClassCastException e) {
            return null;
        }
    }

    public static Object convertType(final kong.unirest.json.JSONObject jsonToConvert) {
        final Set<String> keys = jsonToConvert.keySet();
        final JSONObject jsonObject = new JSONObject();
        for (final String key : keys) {
            final Object o = jsonToConvert.get(key);
            jsonObject.put(key, recurseCheckingConversion(o));
        }
        return jsonObject;
    }

    public static String createLineDivider(final int length) {
        return "\n" + "-".repeat(Math.max(0, length)) + "\n";
    }

    public static ProgressBar createProgressBar(final String taskName) {
        return getProgressBarBuilder(taskName).build();
    }

    public static ProgressBarBuilder getProgressBarBuilder(final String taskName) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(1)
                                       .setMaxRenderedLength(120);
    }

    public static ProgressBar createProgressBar(final String taskName, final long size) {
        return getProgressBarBuilder(taskName, size).build();
    }

    public static ProgressBarBuilder getProgressBarBuilder(final String taskName, final long size) {
        return new ProgressBarBuilder().setTaskName(taskName)
                                       .setInitialMax(size)
                                       .setStyle(ProgressBarStyle.ASCII)
                                       .setUpdateIntervalMillis(1)
                                       .setMaxRenderedLength(120);
    }

    public static String format(final String string) {
        return format(string, 80);
    }

    public static String format(String string, final int columnWrap) {
        final Pattern paragraphs = Pattern.compile("<p>(.*?)</p>");
        final Matcher paragraphMatcher = paragraphs.matcher(string);
        StringBuilder stringBuilder = new StringBuilder();
        while (paragraphMatcher.find()) {
            final String s = paragraphMatcher.group(1);
            stringBuilder.append(s).append("\n");
        }
        string = stringBuilder.toString().strip();
        final Pattern anchors = Pattern.compile("<a.*?>(.*?)</a>");
        final Matcher anchorMatcher = anchors.matcher(string);
        while (anchorMatcher.find()) {
            string = string.replace(anchorMatcher.group(0), anchorMatcher.group(1));
        }
        final String[] noteSplit = string.split("\n");
        stringBuilder = new StringBuilder();
        for (final String s : noteSplit) {
            final String line = WordUtils.wrap(s, columnWrap, "\n", false);
            stringBuilder.append(line).append("\n");
        }
        string = stringBuilder.toString().strip().replace("&amp;", "&");
        return string;
    }

    public static JSONObject formatJson(final JSONObject jsonObjectToFormat) {
        return (JSONObject) recurseCheckingFormat(jsonObjectToFormat);
    }

    private static Object recurseCheckingConversion(final Object object) {
        if (object instanceof kong.unirest.json.JSONObject) {
            final kong.unirest.json.JSONObject o = (kong.unirest.json.JSONObject) object;
            final JSONObject jsonObject = new JSONObject();
            final Set<String> keys = o.keySet();
            for (final String key : keys) {
                final Object o1 = o.get(key);
                jsonObject.put(key, recurseCheckingConversion(o1));
            }
            return jsonObject;
        }
        else if (object instanceof kong.unirest.json.JSONArray) {
            final kong.unirest.json.JSONArray o = (kong.unirest.json.JSONArray) object;
            final JSONArray jsonArray = new JSONArray();
            for (final Object o1 : o) {
                jsonArray.put(recurseCheckingConversion(o1));
            }
            return jsonArray;
        }
        else if (object instanceof String) {
            final String o = (String) object;
            try {
                return Long.parseLong(o);
            }
            catch (final NumberFormatException e) {
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

    private static Object recurseCheckingFormat(final Object object) {
        if (object instanceof JSONObject) {
            final JSONObject o = (JSONObject) object;
            final JSONObject jsonObject = new JSONObject();
            final Set<String> keys = o.keySet();
            for (final String key : keys) {
                final Object o1 = o.get(key);
                jsonObject.put(key, recurseCheckingFormat(o1));
            }
            return jsonObject;
        }
        else if (object instanceof JSONArray) {
            final JSONArray o = (JSONArray) object;
            final JSONArray jsonArray = new JSONArray();
            for (final Object o1 : o) {
                jsonArray.put(recurseCheckingFormat(o1));
            }
            return jsonArray;
        }
        else if (object instanceof String) {
            final String o = (String) object;
            try {
                return Long.parseLong(o);
            }
            catch (final NumberFormatException e) {
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

    public static float round(final float value, final int places) {
        return (float) round((double) value, places);
    }

    public static double round(final double value, final int places) {
        if (places < 0) {
            throw new IllegalArgumentException();
        }
        final BigDecimal bd = BigDecimal.valueOf(value).setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static void shutdownExecutors(final Logger logger, final ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void shutdownUpdaters(final Logger logger, final ScheduledExecutorService scheduledExecutorService) {
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void sleep(final long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
