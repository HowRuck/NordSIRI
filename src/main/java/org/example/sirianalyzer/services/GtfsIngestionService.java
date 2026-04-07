package org.example.sirianalyzer.services;

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

    private final Map<String, String> feeds = Map.of(
        "gtfs.de_realtime-free",
        "https://realtime.gtfs.de/realtime-free.pb",
        "entur_trip-updates",
        "https://api.entur.io/realtime/v1/gtfs-rt/trip-updates",
        "entur_alerts",
        "https://api.entur.io/realtime/v1/gtfs-rt/alerts",
        "entur_vehicle-positions",
        "https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions"
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

        for (var entry : feeds.entrySet()) {
            var feedId = entry.getKey();
            var feedUrl = entry.getValue();

            var entities = gtfsPollingService.pollStream(feedId, feedUrl);
            gtfsKafkaProducer.sendTripUpdates(feedId, entities);

            log.info("----------");
        }

        isRunning.set(false);
    }
}
