package org.hubspot.utils;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Nicholas Curl
 */
public class Response {

    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger();
    private final        String statusText;
    private final        int    statusCode;
    private final        String headers;
    private final        String cookies;
    private final        String body;

    public Response(HttpResponse<JsonNode> response) {
        if (response.getBody() != null) {
            this.body = response.getBody().toPrettyString();
        }
        else {
            this.body = "";
        }
        this.statusCode = response.getStatus();
        if (response.getCookies() != null) {
            this.cookies = response.getCookies().toString();
        }
        else {
            this.cookies = "";
        }
        if (response.getHeaders() != null) {
            this.headers = response.getHeaders().toString();
        }
        else {
            this.headers = "";
        }
        this.statusText = response.getStatusText();
    }

    @Override
    public String toString() {
        return "Response{" +
               "statusText='" + statusText + '\'' +
               ", statusCode=" + statusCode +
               ", headers='" + headers + '\'' +
               ", cookies='" + cookies + '\'' +
               ", body='" + body + '\'' +
               '}';
    }
}
