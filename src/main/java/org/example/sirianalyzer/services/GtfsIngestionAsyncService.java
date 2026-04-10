package org.example.sirianalyzer.services;

import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class GtfsIngestionAsyncService {

    private final GtfsPollingService gtfsPollingService;
    private final GtfsKafkaProducer gtfsKafkaProducer;

    @Async
    public CompletableFuture<Void> processFeedUrlAsync(
        String feedId,
        String feedUrl
    ) {
        var entities = gtfsPollingService.pollStream(feedId, feedUrl);
        var _ = gtfsPollingService.downloadToBytes(feedUrl);
        gtfsKafkaProducer.sendTripUpdates(feedId, entities);
        return CompletableFuture.completedFuture(null);
    }
}
