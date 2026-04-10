package org.example.sirianalyzer.services;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.proto.GtfsNativeFilter;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * Service for polling a GTFS feed
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsPollingService {

    private final RestClient restClient = RestClient.create();
    private final GtfsNativeFilter nativeFilter;

    /**
     * Downloads the GTFS feed from the configured URL and returns it as a byte array
     *
     * @return The GTFS feed as a byte array
     */
    public byte[] downloadToBytes(String feedUrl) {
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

    public List<GtfsNativeFilter.TypedEntity> pollStream(
        String feedId,
        String feedUrl
    ) {
        var startTime = System.currentTimeMillis();
        log.info(
            "Polling GTFS feed stream for feed {} from {}",
            feedId,
            feedUrl
        );

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
                        return nativeFilter.parseNative(
                            feedId,
                            feedUrl,
                            inputStream
                        );
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
