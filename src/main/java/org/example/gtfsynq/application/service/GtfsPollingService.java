package org.example.gtfsynq.application.service;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayInputStream;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.infrastructure.protobuf.GtfsNativeFilter;
import org.example.gtfsynq.util.SizeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * Service for polling a GTFS feed
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsPollingService {

    private final RestClient restClient;
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
        log.debug(
            "Polling GTFS feed stream for feed {} from {}",
            feedId,
            feedUrl
        );

        try {
            return restClient
                .get()
                .uri(feedUrl)
                .exchange((_, response) -> {
                    HttpStatusCode statusCode = response.getStatusCode();

                    if (statusCode == HttpStatus.TOO_MANY_REQUESTS) {
                        var retryAfter = response
                            .getHeaders()
                            .getFirst("Retry-After");

                        log.warn(
                            "Rate limit hit while polling GTFS feed {} from {}",
                            feedId,
                            feedUrl
                        );
                        if (retryAfter != null) {
                            log.warn("Retry-After: {} seconds", retryAfter);
                        }

                        return null;
                    }

                    if (!statusCode.is2xxSuccessful()) {
                        log.warn(
                            "Unexpected HTTP status {} while polling GTFS feed {} from {}",
                            statusCode.value(),
                            feedId,
                            feedUrl
                        );
                        return null;
                    }

                    var responseLength = response
                        .getHeaders()
                        .getContentLength();

                    var responseLengthStr =
                        responseLength != 0
                            ? SizeFormat.humanBytes(responseLength)
                            : "unknown";

                    log.debug(
                        "Received GTFS feed stream of size {} in {} ms",
                        responseLengthStr,
                        System.currentTimeMillis() - startTime
                    );

                    var feedBytes = response.getBody().readAllBytes();

                    try {
                        return nativeFilter.parseNative(
                            feedId,
                            feedUrl,
                            new ByteArrayInputStream(feedBytes)
                        );
                    } catch (InvalidProtocolBufferException firstError) {
                        log.warn(
                            "Failed to parse GTFS feed stream for feed {} from {} on first attempt: {}",
                            feedId,
                            feedUrl,
                            firstError.getMessage()
                        );

                        try {
                            return nativeFilter.parseNative(
                                feedId,
                                feedUrl,
                                new ByteArrayInputStream(feedBytes)
                            );
                        } catch (InvalidProtocolBufferException secondError) {
                            log.warn(
                                "Retry failed while parsing GTFS feed stream for feed {} from {}: {}",
                                feedId,
                                feedUrl,
                                secondError.getMessage()
                            );
                            throw secondError;
                        }
                    } finally {
                        log.debug(
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
