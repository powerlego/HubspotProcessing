package org.hubspot.objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class PropertiesLoader {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    public static List<String> loadProperties(String type) {
        List<String> properties = new LinkedList<>();
        String line;
        InputStream inputStream = PropertiesLoader.class.getResourceAsStream("/properties/" + type + ".txt");
        if (inputStream != null) {
            try {
                InputStreamReader reader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(reader);
                while ((line = bufferedReader.readLine()) != null) {
                    properties.add(line);
                }
                bufferedReader.close();
            } catch (IOException e) {
                logger.fatal("Unable to load properties", e);
            }
        }
        return properties;
    }
}
