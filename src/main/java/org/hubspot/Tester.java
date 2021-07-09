package org.hubspot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.services.HubSpot;
import org.hubspot.utils.CPUMonitor;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * @author Nicholas Curl
 */
public class Tester {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(Tester.class);

    public static void main(String[] args) {

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

        Pattern pattern = Pattern.compile("^(?=.*?Contacts?)(?:(?!Engagements?).)*$");
        CPUMonitor.startMonitoring();
        HubSpot hubspot = new HubSpot("6ab73220-900f-462b-b753-b6757d94cd1d");
        //ArrayList<Long> associated = hubspot.crm().associateDeals(11321890L);
        CPUMonitor.stopMonitoring();
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
