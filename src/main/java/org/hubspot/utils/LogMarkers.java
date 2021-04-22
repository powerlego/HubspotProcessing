package org.hubspot.utils;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

/**
 * @author Nicholas Curl
 */
public enum LogMarkers {
    CORRECTION("CORRECTION"),
    ERROR("ERROR"),
    MISSING("MISSING"),
    CPU_LOAD("CPU_LOAD"),
    DEBUG("DEBUG"),
    DELETION("DELETION"),
    HTTP("HTTP");

    private final Marker marker;

    LogMarkers(String marker) {
        this.marker = MarkerManager.getMarker(marker);
    }

    public Marker getMarker() {
        return marker;
    }
}
