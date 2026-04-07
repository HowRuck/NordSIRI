package org.example.sirianalyzer.services;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class GtfsIngestionService {

    private final GtfsPollingService gtfsPollingService;
    private final GtfsKafkaProducer gtfsKafkaProducer;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final Map<String, List<String>> feeds = Map.of(
        "gtfs.de",
        List.of("https://realtime.gtfs.de/realtime-free.pb"),
        "entur",
        List.of(
            "https://api.entur.io/realtime/v1/gtfs-rt/trip-updates",
            "https://api.entur.io/realtime/v1/gtfs-rt/alerts",
            "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
        )
    );

    @Scheduled(fixedRateString = "${gtfs.fetch.interval-ms}")
    public void process() {
        if (isRunning.get()) {
            log.info(
                "Previous polling is still running, skipping this iteration"
            );
            return;
        }

        isRunning.set(true);
        var startTime = System.currentTimeMillis();

        for (var feedId : feeds.keySet()) {
            var feedUrls = feeds.get(feedId);

            for (var feedUrl : feedUrls) {
                var entities = gtfsPollingService.pollStream(feedId, feedUrl);
                gtfsKafkaProducer.sendTripUpdates(feedId, entities);
            }

            log.info("----------");
        }

        var endTime = System.currentTimeMillis();
        log.info("Total processing time: {}ms", endTime - startTime);

        isRunning.set(false);
    }
}
