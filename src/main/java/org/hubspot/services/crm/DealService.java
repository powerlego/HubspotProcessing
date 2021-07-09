package org.hubspot.services.crm;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Contact;
import org.hubspot.objects.crm.Deal;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.*;
import org.hubspot.utils.exceptions.HubSpotException;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

    public static Path getCacheFolder() {
        return cacheFolder;
    }

    static Deal getById(HttpService service, String propertyString, long id, final RateLimiter rateLimiter)
    throws HubSpotException {
        String urlString = url + id;
        rateLimiter.acquire(1);
        return getDeal(service, propertyString, urlString);
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

    static ArrayList<Long> associateDeals(HttpService httpService, Contact contact, final RateLimiter rateLimiter)
    throws HubSpotException {
        String url = "/crm-associations/v1/associations/" + contact.getId() + "/HUBSPOT_DEFINED/4";
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
                                                                                           contact.getId()),
                                                                                   new StoringRejectedExecutionHandler()
        );
        Utils.addExecutor(threadPoolExecutor);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("DealIdsUpdater_Contact_" +
                                                                                     contact.getId()));
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

    static Deal getDeal(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return parseDealData((JSONObject) httpService.getRequest(url, propertyString));
        }
        catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            }
            else {
                throw e;
            }
        }
    }

    static HashMap<Long, Deal> getUpdatedDeals(HttpService httpService,
                                               PropertyData propertyData,
                                               long lastExecution,
                                               long lastFinished
    ) throws HubSpotException {
        String url = "/crm/v3/objects/deals/search";
        final RateLimiter rateLimiter = RateLimiter.create(3.0);
        ConcurrentHashMap<Long, Deal> deals = new ConcurrentHashMap<>();
        JSONObject body = HubSpotUtils.getUpdateBody(CRMObjectType.DEALS, propertyData, lastExecution, LIMIT);
        long after;
        long count = HubSpotUtils.getUpdateCount(httpService, rateLimiter, CRMObjectType.DEALS, lastExecution);
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        UpdateThreadPoolExecutor threadPoolExecutor = new UpdateThreadPoolExecutor(1,
                                                                                   STARTING_POOL_SIZE,
                                                                                   0L,
                                                                                   TimeUnit.MILLISECONDS,
                                                                                   new LinkedBlockingQueue<>(Math.max(
                                                                                           capacity,
                                                                                           Runtime.getRuntime()
                                                                                                  .availableProcessors()
                                                                                   )),
                                                                                   new CustomThreadFactory(
                                                                                           "DealUpdater"),
                                                                                   new StoringRejectedExecutionHandler(),
                                                                                   cacheFolder,
                                                                                   lastFinished
        );
        Utils.addExecutor(threadPoolExecutor);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("DealUpdaterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getUpdatedDeals", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Deals", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
            threadPoolExecutor.submit(process(deals, pb, jsonObject));
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            body.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
        return new HashMap<>(deals);
    }

    static HashMap<Long, Deal> readDealJsons() throws HubSpotException {
        ConcurrentHashMap<Long, Deal> deals = new ConcurrentHashMap<>();
        File[] files = cacheFolder.toFile().listFiles();
        if (!Utils.isArrayNullOrEmpty(files)) {
            List<File> fileList = Arrays.asList(files);
            Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            int capacity = (int) Math.ceil(Math.ceil((double) fileList.size() / (double) LIMIT) *
                                           Math.pow(MAX_SIZE, -0.6));
            CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                       STARTING_POOL_SIZE,
                                                                                       0L,
                                                                                       TimeUnit.MILLISECONDS,
                                                                                       new LinkedBlockingQueue<>(Math.max(
                                                                                               capacity,
                                                                                               Runtime.getRuntime()
                                                                                                      .availableProcessors()
                                                                                       )),
                                                                                       new CustomThreadFactory(
                                                                                               "DealReader"),
                                                                                       new StoringRejectedExecutionHandler()
            );
            Utils.addExecutor(threadPoolExecutor);
            ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("DealReaderUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "readDealJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            ProgressBar pb = Utils.createProgressBar("Reading Deals", fileList.size());
            Utils.sleep(WARMUP);
            for (List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (File file : partition) {
                        String jsonString = FileUtils.readJsonString(logger, file);
                        JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                        Deal deal = parseDealData(jsonObject);
                        deals.put(deal.getId(), deal);
                        pb.step();
                        Utils.sleep(1);
                    }
                    return null;
                });
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            Utils.shutdownUpdaters(logger, scheduledExecutorService);
            pb.close();
        }
        return new HashMap<>(deals);
    }

    static void writeDealJson(HttpService httpService, PropertyData propertyData, final RateLimiter rateLimiter)
    throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.CONTACTS, rateLimiter);
        int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                 STARTING_POOL_SIZE,
                                                                                 0L,
                                                                                 TimeUnit.MILLISECONDS,
                                                                                 new LinkedBlockingQueue<>(Math.max(
                                                                                         capacity,
                                                                                         Runtime.getRuntime()
                                                                                                .availableProcessors()
                                                                                 )),
                                                                                 new CustomThreadFactory("ContactWriter"),
                                                                                 new StoringRejectedExecutionHandler(),
                                                                                 cacheFolder
        );
        Utils.addExecutor(threadPoolExecutor);
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("ContactWriterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "writeContactJson", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal(LogMarkers.ERROR.getMarker(), "Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ProgressBar pb = Utils.createProgressBar("Writing Contacts", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(() -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject contactJson = (JSONObject) o;
                    contactJson = Utils.formatJson(contactJson);
                    FileUtils.writeJsonCache(cacheFolder, contactJson);
                    pb.step();
                    Utils.sleep(1);
                }
                return null;
            });
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
    }
}
