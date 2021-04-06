package org.hubspot.utils;

import org.apache.commons.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.crm.CRMObjectType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Nicholas Curl
 */
public class Utils {
    public static final Instant now = Instant.now();
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

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
        string = stringBuilder.toString().strip();
        string = string.replace("&amp;", "&");
        return string;
    }

    public static JSONObject formatJson(JSONObject jsonObjectToFormat) {
        return (JSONObject) recurseCheckingFormat(jsonObjectToFormat);
    }

    public static long getObjectCount(HttpService service, CRMObjectType type) {
        JSONObject body = new JSONObject();
        JSONArray filterGroupsArray = new JSONArray();
        JSONObject filters = new JSONObject();
        JSONArray filtersArray = new JSONArray();
        JSONObject filter = new JSONObject();
        filter.put("propertyName", "hs_object_id");
        filter.put("operator", "HAS_PROPERTY");
        filters.put("filters", filtersArray);
        filterGroupsArray.put(filters);
        body.put("filterGroups", filterGroupsArray);
        body.put("properties", "hs_object_id");
        body.put("limit", 1);
        try {
            JSONObject resp = (JSONObject) service.postRequest("/crm/v3/objects/" + type.getValue() + "/search", body);
            return resp.getLong("total");
        } catch (HubSpotException e) {
            logger.fatal("Unable to get object count.", e);
            return 0;
        }
    }

    public static String propertyListToString(List<String> properties) {
        String propertyString = properties.toString();
        propertyString = propertyString.replace("[", "");
        propertyString = propertyString.replace("]", "");
        propertyString = propertyString.replace(" ", "");
        return propertyString;
    }

    public static <T> T convertInstanceObject(Object o, Class<T> clazz) {
        try {
            return clazz.cast(o);
        } catch (ClassCastException e) {
            return null;
        }
    }

    public static String readFile(File file) {
        try {
            FileInputStream inputStream = new FileInputStream(file);
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line;
            StringBuilder fileString = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                line = line.strip();
                fileString.append(line).append("\n");
            }
            return fileString.toString().strip();
        } catch (IOException e) {
            logger.fatal("Unable to read file", e);
            System.exit(-1);
            return "";
        }
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
        } else if (object instanceof kong.unirest.json.JSONArray) {
            kong.unirest.json.JSONArray o = (kong.unirest.json.JSONArray) object;
            JSONArray jsonArray = new JSONArray();
            for (Object o1 : o) {
                jsonArray.put(recurseCheckingConversion(o1));
            }
            return jsonArray;
        } else if (object instanceof String) {
            String o = (String) object;
            try {
                return Long.parseLong(o);
            } catch (NumberFormatException e) {
                if (o.equalsIgnoreCase("true") || o.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(o);
                } else {
                    return o;
                }
            }
        } else {
            return Objects.requireNonNullElse(object, JSONObject.NULL);
        }
    }

    public static void sleep(int seconds) {
        sleep((long) 1000 * seconds);
    }

    public static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        } else if (object instanceof JSONArray) {
            JSONArray o = (JSONArray) object;
            JSONArray jsonArray = new JSONArray();
            for (Object o1 : o) {
                jsonArray.put(recurseCheckingFormat(o1));
            }
            return jsonArray;
        } else if (object instanceof String) {
            String o = (String) object;
            try {
                return Long.parseLong(o);
            } catch (NumberFormatException e) {
                if (o.equalsIgnoreCase("true") || o.equalsIgnoreCase("false")) {
                    return Boolean.parseBoolean(o);
                } else {
                    return o;
                }
            }
        } else {
            return Objects.requireNonNullElse(object, JSONObject.NULL);
        }
    }

    public static <T> T[][] splitArray(T[] arrayToSplit, int chunkSize) {
        if (chunkSize <= 0) {
            return null;
        }
        int rest = arrayToSplit.length % chunkSize;
        int chunks = arrayToSplit.length / chunkSize + (rest > 0 ? 1 : 0);
        Class<?> arrayType = arrayToSplit.getClass().getComponentType();
        @SuppressWarnings("unchecked")
        T[][] arrays = (T[][]) Array.newInstance(arrayType, chunks, 0);
        //T[][] arrays = (T[][]) new T[chunks][];
        for (int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++) {
            arrays[i] = Arrays.copyOfRange(arrayToSplit, i * chunkSize, i * chunkSize + chunkSize);
        }
        if (rest > 0) {
            arrays[chunks - 1] = Arrays.copyOfRange(arrayToSplit, (chunks - 1) * chunkSize, (chunks - 1) * chunkSize + rest);
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
            ArrayList<T> sublist = new ArrayList<>(listToSplit.subList((chunks - 1) * chunkSize, (chunks - 1) * chunkSize + rest));
            lists.add(chunks - 1, sublist);
        }
        return lists;
    }
}
