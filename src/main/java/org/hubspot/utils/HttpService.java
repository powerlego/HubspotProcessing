package org.hubspot.utils;

import com.google.common.base.Strings;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class HttpService {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();

    private final String apiKey;

    public HttpService(String apiKey, String apiBase) {
        this.apiKey = apiKey;
        Unirest.config().automaticRetries(true).socketTimeout(0).defaultBaseUrl(apiBase);
    }

    public Object getRequest(String url, Map<String, Object> queryParams) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest
                        .get(url)
                        .queryString(queryParams)
                        .queryString("hapikey", apiKey)
                        .asJson();
                Object response = getObject(resp);
                if (response != null) return response;
            } catch (UnirestException e) {
                throw new HubSpotException("Can not get data\n URL:" + url, e);
            }
        }
    }

    public Object postRequest(String url, String properties) throws HubSpotException {
        return postRequest(url, properties, "application/json");
    }

    private Object checkResponse(HttpResponse<JsonNode> resp) throws HubSpotException {
        if (204 != resp.getStatus() && 200 != resp.getStatus() && 202 != resp.getStatus()) {
            String message = null;
            try {
                message = (resp.getStatus() == 404) ? resp.getStatusText() : resp.getBody().getObject().getString("message");
            } catch (Exception ignored) {
            }

            if (!Strings.isNullOrEmpty(message)) {
                throw new HubSpotException(message, resp.getStatus());
            } else {
                throw new HubSpotException(resp.getStatusText(), resp.getStatus());
            }
        } else {
            if (resp.getBody() != null) {
                return resp.getBody().isArray() ? resp.getBody().getArray() : resp.getBody().getObject();
            } else {
                return null;
            }
        }
    }

    public Object getRequest(String url, String properties) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest
                        .get(url)
                        .queryString("properties", properties)
                        .queryString("hapikey", apiKey)
                        .asJson();
                Object response = getObject(resp);
                if (response != null) return response;

            } catch (UnirestException e) {
                throw new HubSpotException("Can not get data\n URL:" + url, e);
            }
        }
    }

    public Object getRequest(String url) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest
                        .get(url)
                        .queryString("hapikey", apiKey)
                        .asJson();
                Object response = getObject(resp);
                if (response != null) return response;
            } catch (UnirestException e) {
                throw new HubSpotException("Can not get data\n URL:" + url, e);
            }
        }
    }

    public Object postRequest(String url, String properties, String contentType) throws HubSpotException {
        if (Strings.isNullOrEmpty(contentType)) {
            contentType = "application/json";
        }
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest
                        .post(url)
                        .queryString("hapikey", apiKey)
                        .header("accept", "application/json")
                        .header("Content-Type", contentType)
                        .body(properties)
                        .asJson();
                Object response = getObject(resp);
                if (response != null) return response;
            } catch (UnirestException e) {
                throw new HubSpotException("Cannot make a request: \n" + properties, e);
            }
        }
    }

    private Object getObject(HttpResponse<JsonNode> resp) throws HubSpotException {
        try {
            JSONObject response = (JSONObject) checkResponse(resp);
            if (response != null) {
                return Utils.convertType(response);
            }
        } catch (HubSpotException e) {
            if (e.getCode() == 502) {
                Utils.sleep(2);
            } else if (e.getCode() == 429) {
                Utils.sleep(2);
            } else {
                throw e;
            }
        }
        return null;
    }

    public Object postRequest(String url, org.json.JSONObject properties) throws HubSpotException {
        return postRequest(url, properties.toString(), "application/json");
    }

    public Object postRequest(String url, org.json.JSONObject properties, String contentType) throws HubSpotException {
        return postRequest(url, properties.toString(), contentType);
    }
}
