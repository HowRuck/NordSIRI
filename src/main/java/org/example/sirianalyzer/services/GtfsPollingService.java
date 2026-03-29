package org.example.sirianalyzer.services;

import com.google.common.base.Function;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * Service for polling a GTFS feed
 */
@Service
@Slf4j
public class GtfsPollingService {

    private final RestClient restClient;
    private final String feedUrl;

    /**
     * Creates a new GTFS polling service with the provided feed URL
     *
     * @param feedUrl The URL of the GTFS feed to poll
     */
    public GtfsPollingService(@Value("${gtfs.feed.url}") String feedUrl) {
        this.feedUrl = feedUrl;
        this.restClient = RestClient.create(feedUrl);
    }

    /**
     * Downloads the GTFS feed from the configured URL and returns it as a byte array
     *
     * @return The GTFS feed as a byte array
     */
    public byte[] downloadToBytes() {
        var startTime = System.currentTimeMillis();
        log.info("Downloading GTFS feed from {}", feedUrl);

        try {
            var bytes = restClient
                .get()
                .uri(feedUrl)
                .retrieve()
                .body(byte[].class);

            log.info(
                "Downloaded GTFS feed of size {} in {} ms",
                SizeFormat.humanBytes(bytes.length),
                System.currentTimeMillis() - startTime
            );

            return bytes;
        } catch (Exception e) {
            log.error("Failed to download GTFS feed", e);
            return null;
        }
    }

    /**
     * Polls the GTFS feed stream from the configured URL and processes it using the given processor.
     *
     * @param <T> The type of the result returned by the processor
     * @param processor The function to process the feed stream
     * @return The result of processing the feed stream
     */
    public <T> T pollStream(Function<InputStream, T> processor) {
        var startTime = System.currentTimeMillis();
        log.info("Polling GTFS feed stream from {}", feedUrl);

        try {
            return restClient
                .get()
                .uri(feedUrl)
                // Directly stream the response body to the processor
                .exchange((_, response) -> {
                    var responseLength = response
                        .getHeaders()
                        .getContentLength();

                    log.info(
                        "Received GTFS feed stream of size {} in {} ms",
                        SizeFormat.humanBytes(responseLength),
                        System.currentTimeMillis() - startTime
                    );

                    try (var inputStream = response.getBody()) {
                        return processor.apply(inputStream);
                    } finally {
                        log.info(
                            "Finished processing GTFS feed stream in {} ms",
                            System.currentTimeMillis() - startTime
                        );
                    }
                });
        } catch (Exception e) {
            log.error("Failed to poll GTFS feed stream", e);
            return null;
        }
    }
}
