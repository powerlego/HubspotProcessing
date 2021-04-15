package org.hubspot.services;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.utils.CPUMonitor;
import org.hubspot.utils.ErrorCodes;
import org.hubspot.utils.HttpService;
import org.hubspot.utils.Utils;
import org.hubspot.utils.concurrent.AutoShutdown;
import org.hubspot.utils.concurrent.CustomThreadFactory;
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
    private static final long   WARMUP             = 1000;
    private static final long   LOOP_DELAY         = 2;
    private static final long   UPDATE_INTERVAL    = 1100;
    private static final int    STARTING_POOL_SIZE = 4;
    private static final String debugMessageFormat = "Method %-20s\tProcess Load: %f";

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
        long count = Utils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(STARTING_POOL_SIZE,
                                                                       STARTING_POOL_SIZE,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new CustomThreadFactory("CompanyGrabber")
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newScheduledThreadPool(2, new CustomThreadFactory("CompanyGrabberUpdater"));
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        List<Future<Void>> futures = new ArrayList<>();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getAllCompanies", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime()
                                                                                         .availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Companies", count)) {
            Utils.sleep(WARMUP);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor,
                                                                        process(companies, pb, jsonObject)
                )));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.waitForCompletion(completionService, futures);
            Utils.shutdownExecutors(logger, threadPoolExecutor, cacheFolder);
            scheduledExecutorService.shutdown();
        }
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
                Utils.writeJsonCache(cacheFolder, companyJson);
                companies.put(company.getId(), company);
                pb.step();
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
        JSONObject body = Utils.getUpdateBody(CRMObjectType.COMPANIES, propertyData, lastExecution, LIMIT);
        long after;
        long count = Utils.getUpdateCount(httpService, rateLimiter, CRMObjectType.COMPANIES, lastExecution);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(STARTING_POOL_SIZE,
                                                                       STARTING_POOL_SIZE,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new CustomThreadFactory("CompanyUpdater")
        );
        ScheduledExecutorService scheduledExecutorService
                = Executors.newScheduledThreadPool(2, new CustomThreadFactory("CompanyUpdaterUpdater"));
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        List<Future<Void>> futures = new ArrayList<>();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "getUpdatedCompanies", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime()
                                                                                         .availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try (ProgressBar pb = Utils.createProgressBar("Grabbing and Writing Updated Companies", count)) {
            Utils.sleep(WARMUP);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.postRequest(url, body);
                futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor,
                                                                        process(companies, pb, jsonObject)
                )));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                body.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.waitForCompletion(completionService, futures);
            Utils.shutdownUpdateExecutors(logger, threadPoolExecutor, cacheFolder, lastFinished);
            scheduledExecutorService.shutdown();
        }
        return new HashMap<>(companies);
    }

    static HashMap<Long, Company> readCompanyJsons() throws HubSpotException {
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        File[] files = cacheFolder.toFile().listFiles();
        if (files != null) {
            List<File> fileList = Arrays.asList(files);
            Iterable<List<File>> partitions = Iterables.partition(fileList, LIMIT);
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(STARTING_POOL_SIZE,
                                                                           STARTING_POOL_SIZE,
                                                                           0L,
                                                                           TimeUnit.MILLISECONDS,
                                                                           new LinkedBlockingQueue<>(),
                                                                           new CustomThreadFactory("CompanyReader")
            );
            ScheduledExecutorService scheduledExecutorService
                    = Executors.newScheduledThreadPool(2, new CustomThreadFactory("CompanyReaderUpdater"));
            ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
            List<Future<Void>> futures = new ArrayList<>();
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                double load = CPUMonitor.getProcessLoad();
                String debugMessage = String.format(debugMessageFormat, "readCompanyJsons", load);
                logger.debug(debugMessage);
                int comparison = Double.compare(load, 50.0);
                if (comparison > 0) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                    threadPoolExecutor.setCorePoolSize(numThreads);
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
                else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime()
                                                                                             .availableProcessors()) {
                    int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                    threadPoolExecutor.setCorePoolSize(numThreads);
                    threadPoolExecutor.setMaximumPoolSize(numThreads);
                }
            }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
            try (ProgressBar pb = Utils.createProgressBar("Reading Companies", fileList.size())) {
                Utils.sleep(WARMUP);
                for (List<File> partition : partitions) {
                    futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor, () -> {
                        for (File file : partition) {
                            String jsonString = Utils.readJsonString(logger, file);
                            JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                            Company company = parseCompanyData(jsonObject);
                            companies.put(company.getId(), company);
                            pb.step();
                        }
                        return null;
                    })));
                    Utils.sleep(LOOP_DELAY);
                }
                while (futures.size() > 0) {
                    try {
                        Future<Void> f = completionService.take();
                        futures.remove(f);
                        f.get();
                    }
                    catch (ExecutionException | InterruptedException e) {
                        logger.fatal("Unable to read contact cache", e);
                        System.exit(ErrorCodes.IO_READ.getErrorCode());
                    }
                }
            }
            Utils.shutdownExecutors(logger, threadPoolExecutor);
            scheduledExecutorService.shutdown();
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
        long count = Utils.getObjectCount(httpService, CRMObjectType.COMPANIES, rateLimiter);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(STARTING_POOL_SIZE,
                                                                       STARTING_POOL_SIZE,
                                                                       0L,
                                                                       TimeUnit.MILLISECONDS,
                                                                       new LinkedBlockingQueue<>(),
                                                                       new CustomThreadFactory("CompanyWriter")
        );

        ScheduledExecutorService scheduledExecutorService
                = Executors.newScheduledThreadPool(2, new CustomThreadFactory("CompanyWriterUpdater"));
        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(threadPoolExecutor);
        List<Future<Void>> futures = new ArrayList<>();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            double load = CPUMonitor.getProcessLoad();
            String debugMessage = String.format(debugMessageFormat, "writeCompanyJsons", load);
            logger.debug(debugMessage);
            int comparison = Double.compare(load, 50.0);
            if (comparison > 0) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() - 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
            else if (comparison < 0 && threadPoolExecutor.getMaximumPoolSize() != Runtime.getRuntime()
                                                                                         .availableProcessors()) {
                int numThreads = threadPoolExecutor.getMaximumPoolSize() + 1;
                threadPoolExecutor.setCorePoolSize(numThreads);
                threadPoolExecutor.setMaximumPoolSize(numThreads);
            }
        }, 0, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
        try {
            Files.createDirectories(cacheFolder);
        }
        catch (IOException e) {
            logger.fatal("Unable to create folder {}", cacheFolder, e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Writing Companies", count)) {
            Utils.sleep(WARMUP);
            while (true) {
                rateLimiter.acquire(1);
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                futures.add(completionService.submit(new AutoShutdown<>(threadPoolExecutor, () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject companyJson = (JSONObject) o;
                        companyJson = Utils.formatJson(companyJson);
                        Utils.writeJsonCache(cacheFolder, companyJson);
                        pb.step();
                        Utils.sleep(1L);
                    }
                    return null;
                })));
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
                Utils.sleep(LOOP_DELAY);
            }
            Utils.waitForCompletion(completionService, futures);
            Utils.shutdownExecutors(logger, threadPoolExecutor, cacheFolder);
            scheduledExecutorService.shutdown();
        }
    }
}
