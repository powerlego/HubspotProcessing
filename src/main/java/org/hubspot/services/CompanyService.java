package org.hubspot.services;

import me.tongfei.progressbar.ProgressBar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.objects.PropertyData;
import org.hubspot.objects.crm.CRMObjectType;
import org.hubspot.objects.crm.Company;
import org.hubspot.utils.*;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Nicholas Curl
 */
public class CompanyService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private static final int LIMIT = 10;
    private static final String url = "/crm/v3/objects/companies/";


    static ConcurrentHashMap<Long, Company> getAllCompanies(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.COMPANIES);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("CompanyGrabber"));
        try (ProgressBar pb = Utils.createProgressBar("Grabbing Companies", count)) {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject companyJson = (JSONObject) o;
                        Company company = parseCompanyData(companyJson);
                        companies.put(company.getId(), company);
                        pb.step();
                        Utils.sleep(1L);
                    }
                };
                executorService.submit(process);
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
            }
            Utils.shutdownExecutors(logger, executorService);
        }
        return companies;
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
                    String string = (String) propertyValue;
                    string = string.strip();
                    string = string.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                    company.setProperty(key, string);
                } else {
                    company.setProperty(key, propertyValue);
                }
            } else {
                if (jsonPropertyObject == null) {
                    company.setProperty(key, null);
                } else {
                    if (jsonPropertyObject instanceof String) {
                        String propertyValue = (String) jsonPropertyObject;
                        propertyValue = propertyValue.strip();
                        if (key.equalsIgnoreCase("name")) {
                            propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\:_.\\-@;,]", "");
                        } else {
                            propertyValue = propertyValue.replaceAll("[~`!#$%^&*()+={}\\[\\]|<>?/'\"\\\\]", "");
                        }
                        company.setProperty(key, propertyValue);
                    } else {
                        company.setProperty(key, jsonPropertyObject);
                    }
                }
            }
        }
        company.setData();
        return company;
    }

    static Company getByID(HttpService service, String propertyString, long id) throws HubSpotException {
        String urlString = url + id;
        return getCompany(service, propertyString, urlString);
    }

    static Company getCompany(HttpService httpService, String propertyString, String url) throws HubSpotException {
        try {
            return parseCompanyData((JSONObject) httpService.getRequest(url, propertyString));
        } catch (HubSpotException e) {
            if (e.getMessage().equalsIgnoreCase("Not Found")) {
                return null;
            } else {
                throw e;
            }
        }
    }

    static ConcurrentHashMap<Long, Company> readCompanyJsons() {
        ConcurrentHashMap<Long, Company> companies = new ConcurrentHashMap<>();
        Path jsonFolder = Paths.get("./cache/companies/");
        File[] files = jsonFolder.toFile().listFiles();
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        if (files != null) {
            forkJoinPool.submit(() -> ProgressBar.wrap(Arrays.stream(files).parallel(), Utils.getProgressBarBuilder("Reading Companies")).forEach(file -> {
                String jsonString = Utils.readFile(file);
                JSONObject jsonObject = Utils.formatJson(new JSONObject(jsonString));
                Company company = parseCompanyData(jsonObject);
                companies.put(company.getId(), company);
                Utils.sleep(1L);
            }));
        }
        forkJoinPool.shutdown();
        try {
            if (!forkJoinPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.warn("Termination Timeout");
            }
        } catch (InterruptedException e) {
            logger.fatal("Threads interrupted during wait.", e);
            System.exit(ErrorCodes.THREAD_INTERRUPT_EXCEPTION.getErrorCode());
        }
        return companies;
    }

    static void writeCompanyJson(HttpService httpService, PropertyData propertyData) throws HubSpotException {
        Map<String, Object> map = new HashMap<>();
        map.put("limit", LIMIT);
        map.put("properties", propertyData.getPropertyNamesString());
        map.put("archived", false);
        long after;
        long count = Utils.getObjectCount(httpService, CRMObjectType.COMPANIES);
        ExecutorService executorService = Executors.newFixedThreadPool(20, new CustomThreadFactory("CompanyGrabber"));
        Path jsonFolder = Paths.get("./cache/companies/");
        try {
            Files.createDirectories(jsonFolder);
        } catch (IOException e) {
            logger.fatal("Unable to create folder", e);
            System.exit(ErrorCodes.IO_CREATE_DIRECTORY.getErrorCode());
        }
        try (ProgressBar pb = Utils.createProgressBar("Grabbing Companies", count)) {
            while (true) {
                JSONObject jsonObject = (JSONObject) httpService.getRequest(url, map);
                Runnable process = () -> {
                    for (Object o : jsonObject.getJSONArray("results")) {
                        JSONObject companyJson = (JSONObject) o;
                        companyJson = Utils.formatJson(companyJson);
                        long id = companyJson.has("id") ? companyJson.getLong("id") : 0;
                        Path companyFilePath = jsonFolder.resolve(id + ".json");
                        try {
                            FileWriter fileWriter = new FileWriter(companyFilePath.toFile());
                            fileWriter.write(companyJson.toString(4));
                            fileWriter.close();
                        } catch (IOException e) {
                            logger.fatal("Unable to write file for id " + id, e);
                            System.exit(ErrorCodes.IO_WRITE.getErrorCode());
                        }
                        pb.step();
                        Utils.sleep(1L);
                    }
                };
                executorService.submit(process);
                if (!jsonObject.has("paging")) {
                    break;
                }
                after = jsonObject.getJSONObject("paging").getJSONObject("next").getLong("after");
                map.put("after", after);
            }
            Utils.shutdownExecutors(logger, executorService);
        }
    }
}
