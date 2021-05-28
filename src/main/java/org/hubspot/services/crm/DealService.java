package org.hubspot.services.crm;

import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Deal;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.CacheThreadPoolExecutor;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import org.hubspot.utils.concurrent.StoringRejectedExecutionHandler;
import org.hubspot.utils.exceptions.HubSpotException;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class DealService {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger(DealService.class);
    private static final int    LIMIT              = 10;
    private static final Path   cacheFolder        = Paths.get("./cache/deals/");
    private static final String url                = "/crm/v3/objects/deals/";
    private static final long   WARMUP             = 10;
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    MAX_SIZE           = 50;
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static HashMap<Long, Deal> getAllDeals(HttpService httpService,
                                           PropertyData propertyData,
                                           final RateLimiter rateLimiter
    ) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Deal> deals = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.DEALS, rateLimiter);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        ThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                            STARTING_POOL_SIZE,
                                                                            0L,
                                                                            TimeUnit.MILLISECONDS,
                                                                            new LinkedBlockingQueue<>(Math.max(capacity,
                                                                                                               Runtime.getRuntime()
                                                                                                                      .availableProcessors()
                                                                            )),
                                                                            new CustomThreadFactory("DealGrabber"),
                                                                            new StoringRejectedExecutionHandler(),
                                                                            cacheFolder
        );
        Utils.addExecutor(threadPoolExecutor);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("DealGrabberUpdater"));
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Deals", count);
        Utils.sleep(WARMUP);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getAllDeals", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, 1, TimeUnit.SECONDS);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(process(deals, pb, jsonObject));
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
        return new HashMap<>(deals);
    }

    @NotNull
    private static Callable<Void> process(ConcurrentHashMap<Long, Deal> deals,
                                          ProgressBar pb,
                                          JSONObject jsonObject
    ) {
        return () -> {
            for (Object o : jsonObject.getJSONArray("results")) {
                JSONObject dealData = (JSONObject) o;
                dealData = Utils.formatJson(dealData);
                Deal deal = parseDealData(dealData);
                FileUtils.writeJsonCache(cacheFolder, dealData);
                deals.put(deal.getId(), deal);
                pb.step();
                Utils.sleep(1);
            }
            return null;
        };
    }

    static Deal parseDealData(JSONObject jsonObject) {
        long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        Deal deal = new Deal(id);
        JSONObject jsonProperties = jsonObject.getJSONObject("properties");
        Set<String> keys = jsonProperties.keySet();
        for (String key : keys) {
            Object jsonPropertyObject = jsonProperties.get(key);
            if (jsonPropertyObject instanceof JSONObject) {
                JSONObject jsonProperty = (JSONObject) jsonPropertyObject;
                Object propertyValue = jsonProperty.get("value");
                if (propertyValue instanceof String) {
                    String string = ((String) propertyValue).strip()
                                                            .replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                    deal.setProperty(key, string);
                }
                else {
                    deal.setProperty(key, propertyValue);
                }
            }
            else if (jsonPropertyObject == null) {
                deal.setProperty(key, null);
            }
            else if (jsonPropertyObject instanceof String) {
                String propertyValue = ((String) jsonPropertyObject).strip();
                if (key.equalsIgnoreCase("firstname")) {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\:_.\\-@;,]", "");
                }
                else {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                }
                deal.setProperty(key, propertyValue);
            }
            else {
                deal.setProperty(key, jsonPropertyObject);
            }
        }
        deal.setData(deal.toJson());
        return deal;
    }

    public static Path getCacheFolder() {
        return cacheFolder;
    }
}
