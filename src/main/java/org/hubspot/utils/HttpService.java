package org.hubspot.utils;

import com.google.common.base.Strings;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.json.JSONObject;
import org.apache.http.impl.execchain.RequestAbortedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.exceptions.HubSpotException;

import java.util.Map;

/**
 * @author Nicholas Curl
 */
public class HttpService {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(HttpService.class);
    private final        String apiKey;

    public HttpService(String apiKey, String apiBase) {
        this.apiKey = apiKey;
        Unirest.config().automaticRetries(true).socketTimeout(0).defaultBaseUrl(apiBase).connectTimeout(0);
    }

    public Object getRequest(String url, Map<String, Object> queryParams) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest.get(url)
                                                     .queryString(queryParams)
                                                     .queryString("hapikey", apiKey)
                                                     .asJson();

                Object response = getObject(resp);
                if (response != null) {
                    return response;
                }
            }
            catch (UnirestException e) {
                if (!(e.getCause() instanceof RequestAbortedException)) {
                    throw new HubSpotException("Can not get data\n URL:" + url,
                                               ErrorCodes.UNIREST_EXCEPTION.getErrorCode(),
                                               e
                    );
                }
            }
        }
    }

    public Object getRequest(String url, String properties) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest.get(url)
                                                     .queryString("properties", properties)
                                                     .queryString("hapikey", apiKey)
                                                     .asJson();
                Object response = getObject(resp);
                if (response != null) {
                    return response;
                }
            }
            catch (UnirestException e) {
                if (!(e.getCause() instanceof RequestAbortedException)) {
                    throw new HubSpotException("Can not get data\n URL:" + url,
                                               ErrorCodes.UNIREST_EXCEPTION.getErrorCode(),
                                               e
                    );
                }
            }
        }
    }

    public Object getRequest(String url) throws HubSpotException {
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest.get(url).queryString("hapikey", apiKey).asJson();
                Object response = getObject(resp);
                if (response != null) {
                    return response;
                }
            }
            catch (UnirestException e) {
                if (!(e.getCause() instanceof RequestAbortedException)) {
                    throw new HubSpotException("Can not get data\n URL:" + url,
                                               ErrorCodes.UNIREST_EXCEPTION.getErrorCode(),
                                               e
                    );
                }
            }
        }
    }

    public Object postRequest(String url, String properties) throws HubSpotException {
        return postRequest(url, properties, "application/json");
    }

    public Object postRequest(String url, String properties, String contentType) throws HubSpotException {
        if (Strings.isNullOrEmpty(contentType)) {
            contentType = "application/json";
        }
        while (true) {
            try {
                HttpResponse<JsonNode> resp = Unirest.post(url)
                                                     .queryString("hapikey", apiKey)
                                                     .header("accept", "application/json")
                                                     .header("Content-Type", contentType)
                                                     .body(properties)
                                                     .asJson();
                Object response = getObject(resp);
                if (response != null) {
                    return response;
                }
            }
            catch (UnirestException e) {
                if (!(e.getCause() instanceof RequestAbortedException)) {
                    throw new HubSpotException("Cannot make a request: \n" + properties,
                                               ErrorCodes.UNIREST_EXCEPTION.getErrorCode(),
                                               e
                    );
                }
            }
        }
    }

    public Object postRequest(String url, org.json.JSONObject properties) throws HubSpotException {
        return postRequest(url, properties.toString(), "application/json");
    }

    public Object postRequest(String url, org.json.JSONObject properties, String contentType) throws HubSpotException {
        return postRequest(url, properties.toString(), contentType);
    }

    private Object checkResponse(HttpResponse<JsonNode> resp) throws HubSpotException {
        logger.debug(LogMarkers.HTTP.getMarker(), new Response(resp));
        if (204 != resp.getStatus() && 200 != resp.getStatus() && 202 != resp.getStatus()) {
            String message = null;
            try {
                message = resp.getStatus() == 429
                          ? resp.getBody().getObject().getString("message")
                          : resp.getStatusText();
            }
            catch (Exception ignored) {
            }
            if (!Strings.isNullOrEmpty(message)) {
                if (resp.getStatus() == 429) {
                    String policyName = resp.getBody().getObject().has("policyName")
                                        ? resp.getBody()
                                              .getObject()
                                              .getString("policyName")
                                        : "";
                    throw new HubSpotException(resp.getStatus() + " " + message,
                                               policyName,
                                               ErrorCodes.HTTP_429.getErrorCode()
                    );
                }
                else {
                    throw new HubSpotException(resp.getStatus() + " " + message, resp.getStatus() % 256);
                }
            }
            else {
                throw new HubSpotException(resp.getStatus() + " " + resp.getStatusText(), resp.getStatus() % 256);
            }
        }
        else if (resp.getBody() != null) {
            return resp.getBody().isArray() ? resp.getBody().getArray() : resp.getBody().getObject();
        }
        else {
            return null;
        }
    }

    private Object getObject(HttpResponse<JsonNode> resp) throws HubSpotException {
        try {
            JSONObject response = (JSONObject) checkResponse(resp);
            if (response != null) {
                return Utils.convertType(response);
            }
        }
        catch (HubSpotException e) {
            if (e.getCode() == ErrorCodes.HTTP_502.getErrorCode() ||
                e.getCode() == ErrorCodes.HTTP_503.getErrorCode() ||
                e.getCode() == ErrorCodes.HTTP_504.getErrorCode() ||
                e.getCode() == ErrorCodes.HTTP_500.getErrorCode() ||
                e.getCode() == ErrorCodes.HTTP_524.getErrorCode() ||
                e.getCode() == ErrorCodes.HTTP_409.getErrorCode()) {
                Utils.sleep(50L);
            }
            else if (e.getCode() == ErrorCodes.HTTP_429.getErrorCode()) {
                if (e.getPolicyName().equalsIgnoreCase("DAILY")) {
                    throw new HubSpotException("Daily limit reached", ErrorCodes.DAILY_LIMIT_REACHED.getErrorCode());
                }
                else {
                    Utils.sleep(50L);
                }
            }
            else {
                throw e;
            }
        }
        return null;
    }
}
