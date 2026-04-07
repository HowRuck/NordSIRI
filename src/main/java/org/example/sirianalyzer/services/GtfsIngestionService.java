package org.example.sirianalyzer.services;

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

    @Scheduled(fixedRateString = "${gtfs.fetch.interval-ms}")
    public void process() {
        if (isRunning.get()) {
            log.info(
                "Previous polling is still running, skipping this iteration"
            );
            return;
        }

        isRunning.set(true);

        var entities = gtfsPollingService.pollStream();
        var _ = gtfsPollingService.downloadToBytes();

        gtfsKafkaProducer.sendTripUpdates("testFeed", entities);

        isRunning.set(false);
    }
}
