package org.hubspot.objects;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Nicholas Curl
 */
public enum CacheFolders {
    CONTACTS(Paths.get("./cache/contacts/")),
    ENGAGEMENTS(Paths.get("./cache/engagements/engagements/")),
    ENGAGEMENT_IDS(Paths.get("./cache/engagements/ids")),
    PROPERTIES(Paths.get("./cache/properties/"));

    private final Path path;

    CacheFolders(Path path) {
        this.path = path;
    }

    public Path getValue() {
        return path;
    }
}
