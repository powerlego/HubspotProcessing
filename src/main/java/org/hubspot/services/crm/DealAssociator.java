package org.hubspot.services.crm;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.Utils;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.CustomThreadPoolExecutor;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;
import org.hubspot.utils.exceptions.HubSpotException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class DealAssociator {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(DealAssociator.class);

    private static final RateLimiter rateLimiter        = RateLimiter.create(13.0);
    private static final int         LIMIT              = 10;
    private static final long        UPDATE_INTERVAL    = 100;
    private static final int         STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final String      debugMessageFormat = "Method %-30s\tProcess Load: %f";
    private static final int         MAX_SIZE           = 50;


    static ArrayList<Long> associateDeals(HttpService httpService, long contactId) throws HubSpotException {
        String url = "/crm-associations/v1/associations/" + contactId + "/HUBSPOT_DEFINED/4";
        Map<String, Object> queryParam = new HashMap<>();
        queryParam.put("limit", LIMIT);
        List<Long> dealIds = Collections.synchronizedList(new ArrayList<>());
        long offset;
        CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                   STARTING_POOL_SIZE,
                                                                                   0L,
                                                                                   TimeUnit.MILLISECONDS,
                                                                                   new LinkedBlockingQueue<>(200),
                                                                                   new CustomThreadFactory(
                                                                                           "DealIds_Contact_" +
                                                                                           contactId),
                                                                                   new StoringRejectedExecutionHandler()
        );
        Utils.addExecutor(threadPoolExecutor);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("DealIdsUpdater_Contact_" +
                                                                                     contactId));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getAllDealIds", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, queryParam);
            JSONArray jsonDealIds = jsonObject.getJSONArray("results");
            threadPoolExecutor.submit(() -> {
                for (int i = 0; i < jsonDealIds.length(); i++) {
                    dealIds.add(jsonDealIds.getLong(i));
                    Utils.sleep(1);
                }
                return null;
            });
            if (!jsonObject.getBoolean("hasMore")) {
                break;
            }
            offset = jsonObject.getLong("offset");
            queryParam.put("offset", offset);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        return new ArrayList<>(dealIds);
    }

}
