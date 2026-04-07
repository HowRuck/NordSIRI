package org.example.sirianalyzer.config;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gtfs")
public class GtfsConfig {

    private Map<String, List<String>> feeds;

    public Map<String, List<String>> getFeeds() {
        return feeds;
    }

    public void setFeeds(Map<String, List<String>> feeds) {
        this.feeds = feeds;
    }
}
