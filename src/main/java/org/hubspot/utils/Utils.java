package org.hubspot.utils;

import org.apache.commons.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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
            stringBuilder.append(string).append("\n");
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

    public static String propertyListToString(List<String> properties){
        String propertyString = properties.toString();
        propertyString = propertyString.replace("[", "");
        propertyString = propertyString.replace("]","");
        propertyString = propertyString.replace(" ","");
        return propertyString;
    }

    public static void sleep(long milliseconds){
        try{
            Thread.sleep(milliseconds);
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    public static void sleep(int seconds){
        sleep((long)1000*seconds);
    }
}
