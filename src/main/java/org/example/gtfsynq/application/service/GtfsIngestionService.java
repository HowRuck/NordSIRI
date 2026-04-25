package org.example.gtfsynq.application.service;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.infrastructure.config.GtfsConfig;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@AllArgsConstructor
public class GtfsIngestionService {

    private final GtfsConfig gtfsConfig;
    private final GtfsIngestionAsyncService gtfsIngestionAsyncService;

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
        var startTime = System.currentTimeMillis();

        try {
            var feeds = gtfsConfig.getFeeds();

            for (var feedId : feeds.keySet()) {
                var feedUrls = feeds.get(feedId);

                var futures = new ArrayList<CompletableFuture<Void>>(
                    feedUrls.size()
                );

                for (var feedUrl : feedUrls) {
                    futures.add(
                        gtfsIngestionAsyncService.processFeedUrlAsync(
                            feedId,
                            feedUrl
                        )
                    );
                }

                CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
                ).join();
            }

            var endTime = System.currentTimeMillis();
            log.info("Total processing time: {}ms", endTime - startTime);
        } finally {
            isRunning.set(false);
        }
    }
}
