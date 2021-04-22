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

    static HashMap<Long, Company> getAllCompanies(final HttpService httpService,
                                                  final PropertyData propertyData,
                                                  final RateLimiter rateLimiter
    ) throws HubSpotException {
        final Map<String, Object> map = new HashMap<>();
        final ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        final long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
        final int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        final CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
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
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyGrabberUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getAllCompanies", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (final IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        final ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            final JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
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
    private static Callable<Void> process(final ConcurrentHashMap<Long, Company> companies,
                                          final ProgressBar pb,
                                          final JSONObject jsonObject
    ) {
        return () -> {
            for (final Object o : jsonObject.getJSONArray("results")) {
                JSONObject companyJson = (JSONObject) o;
                companyJson = Utils.formatJson(companyJson);
                final Company company = parseCompanyData(companyJson);
                FileUtils.writeJsonCache(cacheFolder, companyJson);
                companies.put(company.getId(), company);
                pb.step();
                Utils.sleep(1);
            }
            return null;
        };
    }

    static Company parseCompanyData(final JSONObject jsonObject) {
        final long id = jsonObject.has("id") ? jsonObject.getLong("id") : 0;
        final Company company = new Company(id);
        final JSONObject jsonProperties = jsonObject.getJSONObject("properties");
        final Set<String> keys = jsonProperties.keySet();
        for (final String key : keys) {
            final Object jsonPropertyObject = jsonProperties.get(key);
            if (jsonPropertyObject instanceof JSONObject) {
                final JSONObject jsonProperty = (JSONObject) jsonPropertyObject;
                final Object propertyValue = jsonProperty.get("value");
                if (propertyValue instanceof String) {
                    final String string = ((String) propertyValue).strip()
                                                                  .replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]",
                                                                              ""
                                                                  );
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

    static Company getByID(final HttpService service,
                           final String propertyString,
                           final long id,
                           final RateLimiter rateLimiter
    )
    throws HubSpotException {
        final String urlString = url + id;
        rateLimiter.acquire(1);
        return getCompany(service, propertyString, urlString);
    }

    static Company getCompany(final HttpService httpService, final String propertyString, final String url)
    throws HubSpotException {
        try {
            return parseCompanyData((JSONObject) httpService.getRequest(url, propertyString));
        }
        catch (final HubSpotException e) {
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

    static HashMap<Long, Company> getUpdatedCompanies(final HttpService httpService,
                                                      final PropertyData propertyData,
                                                      final long lastExecution,
                                                      final long lastFinished
    ) throws HubSpotException {
        final String url = "/crm/v3/objects/companies/search";
        final RateLimiter rateLimiter = RateLimiter.create(3.0);
        final ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        final JSONObject body = HubSpotUtils.getUpdateBody(CRMObjectType.COMPANIES, propertyData, lastExecution, LIMIT);
        long after;
        final long count = HubSpotUtils.getUpdateCount(httpService,
                                                       rateLimiter,
                                                       CRMObjectType.COMPANIES,
                                                       lastExecution
        );
        final int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        final UpdateThreadPoolExecutor threadPoolExecutor = new UpdateThreadPoolExecutor(1,
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
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyUpdaterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "getUpdatedCompanies", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        final ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            final JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
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
        final ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        final File[] files = cacheFolder.toFile().listFiles();
        if (files != null) {
            final List<File> fileList = Arrays.asList(files);
            final Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            final int capacity = (int) Math.ceil(Math.ceil((double) fileList.size() / (double) LIMIT) *
                                                 Math.pow(MAX_SIZE, -0.6));
            final CustomThreadPoolExecutor threadPoolExecutor = new CustomThreadPoolExecutor(1,
                                                                                             STARTING_POOL_SIZE,
                                                                                             0L,
                                                                                             TimeUnit.MILLISECONDS,
                                                                                             new LinkedBlockingQueue<>(
                                                                                                     Math.max(
                                                                                                             capacity,
                                                                                                             Runtime.getRuntime()
                                                                                                                    .availableProcessors()
                                                                                                     )),
                                                                                             new CustomThreadFactory(
                                                                                                     "CompanyReader"),
                                                                                             new StoringRejectedExecutionHandler()
            );
            final ScheduledExecutorService scheduledExecutorService
                    = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyReaderUpdater"));
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                final double load = CPUMonitor.getProcessLoad();
                final String debugMessage = String.format(debugMessageFormat, "readCompanyJsons", load);
                Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            final ProgressBar pb = Utils.createProgressBar("Reading Companies", fileList.size());
            Utils.sleep(WARMUP);
            for (final List<File> partition : partitions) {
                threadPoolExecutor.submit(() -> {
                    for (final File file : partition) {
                        final String jsonString = FileUtils.readJsonString(logger, file);
                        final JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                        final Company company = parseCompanyData(jsonObject);
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

    static void writeCompanyJsons(final HttpService httpService,
                                  final PropertyData propertyData,
                                  final RateLimiter rateLimiter
    )
    throws HubSpotException {
        final Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        final long count = HubSpotUtils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
        final int capacity = (int) Math.ceil(Math.ceil((double) count / (double) LIMIT) * Math.pow(MAX_SIZE, -0.6));
        final CacheThreadPoolExecutor threadPoolExecutor = new CacheThreadPoolExecutor(1,
                                                                                       STARTING_POOL_SIZE,
                                                                                       0L,
                                                                                       TimeUnit.MILLISECONDS,
                                                                                       new LinkedBlockingQueue<>(Math.max(
                                                                                               capacity,
                                                                                               Runtime.getRuntime()
                                                                                                      .availableProcessors()
                                                                                       )),
                                                                                       new CustomThreadFactory(
                                                                                               "CompanyWriter"),
                                                                                       new StoringRejectedExecutionHandler(),
                                                                                       cacheFolder
        );
        final ScheduledExecutorService scheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(new CustomThreadFactory("CompanyWriterUpdater"));
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            final double load = CPUMonitor.getProcessLoad();
            final String debugMessage = String.format(debugMessageFormat, "writeCompanyJsons", load);
            Utils.adjustLoad(threadPoolExecutor, load, debugMessage, logger, MAX_SIZE);
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (final IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        final ProgressBar pb = Utils.createProgressBar("Writing Companies", count);
        Utils.sleep(WARMUP);
        while (true) {
            rateLimiter.acquire(1);
            final JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
            threadPoolExecutor.submit(() -> {
                for (final Object o : jsonObject.getJSONArray("results")) {
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
