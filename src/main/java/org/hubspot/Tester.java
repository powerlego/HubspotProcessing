package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Base64;

/**
 * @author Nicholas Curl
 */
public class Tester {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(Tester.class);

    public static void main(String[] args) throws SQLException, IOException {
        String data
                = "data:image/png;base64,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";
        String replacement = data.replaceAll("^\\S+base64,", "");
        byte[] bytes = Base64.getDecoder().decode(replacement);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        FileOutputStream fileOutputStream = new FileOutputStream(Paths.get("./contacts/test.png").toFile());
        fileOutputStream.write(bytes);
        fileOutputStream.close();
        /*BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setPoolPreparedStatements(true);
        basicDataSource.setUrl("jdbc:postgresql://localhost:5432/hubspot_data");
        basicDataSource.setUsername("postgres");
        basicDataSource.setPassword("oneplusone=2");
        ExecutorService executorService = CustomExecutors.newFixedThreadPool(20);
        ProgressBar progressBar = Utils.createProgressBar("Testing", 20);
        for (int i = 0; i < 20; i++) {
            executorService.submit(() -> {
                                       Connection connection = basicDataSource.getConnection();
                                       PreparedStatement preparedStatement = connection.prepareStatement("insert into test(data) values (?)");
                                       preparedStatement.setBinaryStream(1, new FileInputStream(Paths.get(
                                               "C:\\Users\\nicho\\Documents\\mahaffey2.bmp").toFile()));
                                       preparedStatement.execute();
                                       preparedStatement.close();
                                       connection.close();
                                       progressBar.step();
                                       return null;
                                   }
            );
        }*/
        /*CPUMonitor.startMonitoring();
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        HashMap<Long, Contact> contacts = hubspot.crm().readContactJsons();

        CPUMonitor.stopMonitoring();*/
        /*Utils.shutdownExecutors(logger,executorService);
        progressBar.close();
        basicDataSource.close();*/
        /*
        try {
            Files.createDirectories(Paths.get("./cache"));
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to create cache folder {}", Paths.get("./cache"), e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }

        HashMap<Long, Deal> deals = hubspot.crm().getAllDeals("dealinformation", true);
        CPUMonitor.stopMonitoring();*/
    }
}
