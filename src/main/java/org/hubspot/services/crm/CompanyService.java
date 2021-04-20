package org.hubspot.services.crm;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.utils.*;
import org.hubspot.utils.concurrent.*;
import org.hubspot.utils.exceptions.HubSpotException;
import org.jetbrains.annotations.NotNull;
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
public class CompanyService {

    /**
     * The instance of the logger
     */
    private static final Logger logger             = LogManager.getLogger();
    private static final int    LIMIT              = 10;
    private static final String url                = "/crm/v3/objects/companies/";
    private static final Path   cacheFolder        = Paths.get("./cache/companies/");
    private static final long   WARMUP             = 10;
    private static final long   UPDATE_INTERVAL    = 100;
    private static final int    MAX_SIZE           = 50;
    private static final int    STARTING_POOL_SIZE = Runtime.getRuntime().availableProcessors();
    private static final String debugMessageFormat = "Method %-30s\tProcess Load: %f";

    public static boolean cacheExists() {
        return cacheFolder.toFile().exists();
    }

    static HashMap<Long, Company> getAllCompanies(HttpService httpService,
                                                  PropertyData propertyData,
                                                  final RateLimiter rateLimiter
    ) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
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
                                                                                 new CustomThreadFactory(
                                                                                         "CompanyGrabber"),
                                                                                 new StoringRejectedExecutionHandler(),
                                                                                 cacheFolder
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyGrabberUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getAllCompanies", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(process(companies, pb, jsonObject));
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            map.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
        return new HashMap<>(companies);
    }

    @NotNull
    private static Callable<Void> process(ConcurrentHashMap<Long, Company> companies,
                                          ProgressBar pb,
                                          JSONObject jsonObject
    ) {
        return () -> {
            for (Object o : jsonObject.getJSONArray("results")) {
                JSONObject companyJson = (JSONObject) o;
                companyJson = Utils.formatJson(companyJson);
                Company company = parseCompanyData(companyJson);
                FileUtils.writeJsonCache(cacheFolder, companyJson);
                companies.put(company.getId(), company);
                pb.step();
                Utils.sleep(1);
            }
            return null;
        };
    }

    static Company parseCompanyData(JSONObject jsonObject) {
        long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        Company company = new Company(id);
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
                    company.setProperty(key, string);
                }
                else {
                    company.setProperty(key, propertyValue);
                }
            }
            else if (jsonPropertyObject == null) {
                company.setProperty(key, null);
            }
            else if (jsonPropertyObject instanceof String) {
                String propertyValue = ((String) jsonPropertyObject).strip();
                if (key.equalsIgnoreCase("name")) {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\:_.\\-@;,]", "");
                }
                else {
                    propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                }
                company.setProperty(key, propertyValue);
            }
            else {
                company.setProperty(key, jsonPropertyObject);
            }
        }
        company.setData();
        return company;
    }

    static Company getByID(HttpService service, String propertyString, long id, final RateLimiter rateLimiter)
    throws HubSpotException {
        String urlString = url + id;
        rateLimiter.acquire(1);
        return getCompany(service, propertyString, urlString);
    }

    static Company getCompany(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return parseCompanyData((JSONObject) httpService.getRequest(url, propertyString));
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

    public static Path getCacheFolder() {
        return cacheFolder;
    }

    static HashMap<Long, Company> getUpdatedCompanies(HttpService httpService,
                                                      PropertyData propertyData,
                                                      long lastExecution,
                                                      long lastFinished
    ) throws HubSpotException {
        String url = "/crm/v3/objects/companies/search";
        final RateLimiter rateLimiter = RateLimiter.create(3.0);
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        JSONObject body = HubSpotUtils.getUpdateBody(CRMObjectType.COMPANIES, propertyData, lastExecution, LIMIT);
        long after;
        long count = HubSpotUtils.getUpdateCount(httpService, rateLimiter, CRMObjectType.COMPANIES, lastExecution);
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
                                                                                           "CompanyUpdater"),
                                                                                   new StoringRejectedExecutionHandler(),
                                                                                   cacheFolder,
                                                                                   lastFinished
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyUpdaterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getUpdatedCompanies", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
            threadPoolExecutor.submit(process(companies, pb, jsonObject));
            if (!jsonObject.has("paging")) {
                break;
            }
            after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
            body.put("after", after);
        }
        Utils.shutdownExecutors(logger, threadPoolExecutor);
        Utils.shutdownUpdaters(logger, scheduledExecutorService);
        pb.close();
        return new HashMap<>(companies);
    }

    static HashMap<Long, Company> readCompanyJsons() throws HubSpotException {
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        File[] files = cacheFolder.toFile().listFiles();
        if (files != null) {
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
                                                                                               "CompanyReader"),
                                                                                       new StoringRejectedExecutionHandler()
            );
            ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyReaderUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "readCompanyJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            ProgressBar pb = Utils.createProgressBar("Reading Companies", fileList.size());
            Utils.sleep(WARMUP);
            for (List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (File file : partition) {
                        String jsonString = FileUtils.readJsonString(logger, file);
                        JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                        Company company = parseCompanyData(jsonObject);
                        companies.put(company.getId(), company);
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
        return new HashMap<>(companies);
    }

    static void writeCompanyJsons(HttpService httpService, PropertyData propertyData, final RateLimiter rateLimiter)
    throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
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
                                                                                 new CustomThreadFactory("CompanyWriter"),
                                                                                 new StoringRejectedExecutionHandler(),
                                                                                 cacheFolder
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyWriterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "writeCompanyJsons", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        ProgressBar pb = Utils.createProgressBar("Writing Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(() -> {
                for (Object o : jsonObject.getJSONArray("results")) {
                    JSONObject companyJson = (JSONObject) o;
                    companyJson = Utils.formatJson(companyJson);
                    FileUtils.writeJsonCache(cacheFolder, companyJson);
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
