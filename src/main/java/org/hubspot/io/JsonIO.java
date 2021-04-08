package org.hubspot.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.Utils;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nicholas Curl
 */
public class JsonIO {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final Path folder = Paths.get("./jsons/");

    public static List<JSONObject> read() {
        List<JSONObject> jsonObjects = new LinkedList<>();
        File[] files = folder.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                try {
                    InputStreamReader input = new InputStreamReader(new FileInputStream(file));
                    BufferedReader bufferedReader = new BufferedReader(input);
                    String line;
                    StringBuilder builder = new StringBuilder();
                    while ((line = bufferedReader.readLine()) != null) {
                        line = line.strip();
                        builder.append(line).append("\n");
                    }
                    jsonObjects.add(Utils.formatJson(new JSONObject(builder.toString())));
                } catch (IOException e) {
                    logger.fatal("Cannot read file.", e);
                    System.exit(ErrorCodes.IO_READ.getErrorCode());
                }
            }
        }
        return jsonObjects;
    }

    public static void write(JSONObject jsonObject) {
        long id = jsonObject.getLong("id");
        try {
            Files.createDirectories(folder);
        } catch (IOException e) {
            logger.fatal("Unable to create engagements folder", e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        Path filePath = folder.resolve(id + ".json");
        try {
            FileWriter writer = new FileWriter(filePath.toFile());
            writer.write(jsonObject.toString(4));
            writer.close();
        } catch (IOException e) {
            logger.fatal("Unable to write to file", e);
            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
        }
    }
}
