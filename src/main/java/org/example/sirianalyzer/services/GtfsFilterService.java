package org.example.sirianalyzer.services;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.services.filtering.GtfsFilterWorkerService;
import org.example.sirianalyzer.services.filtering.GtfsFilterWorkerService.BatchEntity;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterService {

    public static final int BATCH_SIZE = 10_000;
    public static final int ENTITY_FIELD_NUMBER = 2;

    private final ExecutorService executorService =
        Executors.newVirtualThreadPerTaskExecutor();

    private final MeterRegistry registry;
    private final GtfsFilterWorkerService workerService;

    /**
     * Filters a GTFS feed stream, returning a list of confirmed updates
     *
     * <p>
     * This method reads the raw GTFS feed stream, filters out entities that have
     * not changed since the last feed, and returns a list of confirmed updates.
     * </p>
     *
     * @param feedId The feed identifier
     * @param rawStream The raw GTFS feed stream
     *
     * @return A list of confirmed updates
     *
     * @throws IOException If an error occurs while reading the stream
     */
    public List<BatchEntity> filter(String feedId, InputStream rawStream)
        throws IOException {
        var timerSample = Timer.start(registry);

        try {
            var cis = CodedInputStream.newInstance(
                new BufferedInputStream(rawStream)
            );

            var confirmedUpdates = processEntitiesInBatches(cis);

            logSummary(feedId, lastSeenCount, confirmedUpdates, 0L);

            return confirmedUpdates;
        } finally {
            timerSample.stop(
                registry.timer("gtfs.filter.latency", "source", feedId)
            );
        }
    }

    private int lastSeenCount;

    /**
     * Reads entities from the CodedInputStream and processes them in batches
     *
     * @param cis The CodedInputStream to read from
     * @return List of confirmed updates after filtering
     * @throws IOException If an error occurs while reading from the CodedInputStream
     */
    private List<BatchEntity> processEntitiesInBatches(CodedInputStream cis)
        throws IOException {
        var seen = 0;
        var batch = new ArrayList<ByteString>(BATCH_SIZE);
        var batchFutures = new ArrayList<
            CompletableFuture<List<BatchEntity>>
        >();

        var feedId = "testing";

        while (!cis.isAtEnd()) {
            var tag = cis.readTag();

            if (WireFormat.getTagFieldNumber(tag) != ENTITY_FIELD_NUMBER) {
                cis.skipField(tag);
                continue;
            }

            seen++;
            batch.add(cis.readBytes());

            if (batch.size() == BATCH_SIZE) {
                submitBatchForProcessing(feedId, batch, batchFutures);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            submitBatchForProcessing(feedId, batch, batchFutures);
        }

        return consolidateBatchResults(batchFutures, seen);
    }

    /**
     * Processes a batch asynchronously and adds the future to the list
     */
    private void submitBatchForProcessing(
        String feedId,
        List<ByteString> batch,
        List<CompletableFuture<List<BatchEntity>>> batchFutures
    ) {
        var batchCopy = new ArrayList<>(batch);
        var future = CompletableFuture.supplyAsync(
            () -> workerService.processBatch(feedId, batchCopy),
            executorService
        );
        batchFutures.add(future);
    }

    /**
     * Collects and consolidates results from all batch futures
     */
    private List<BatchEntity> consolidateBatchResults(
        List<CompletableFuture<List<BatchEntity>>> batchFutures,
        int seen
    ) {
        var confirmedUpdates = Collections.synchronizedList(
            new ArrayList<BatchEntity>(lastSeenCount)
        );

        var startNanos = System.nanoTime();
        for (var future : batchFutures) {
            confirmedUpdates.addAll(future.join());
        }
        var totalStallNanos = System.nanoTime() - startNanos;

        log.info(
            "Thread consolidation finished with stall time: {}ms",
            totalStallNanos / 1_000_000L
        );

        lastSeenCount = seen;
        return confirmedUpdates;
    }

    /**
     * Logs a summary of the filter results, including stall time and trip update counts.
     *
     * @param feedId the feed ID to log
     * @param totalSeen the total number of entities seen
     * @param confirmedUpdates the list of confirmed updates
     * @param totalStallNanos the total stall time in nanoseconds
     */
    private void logSummary(
        String feedId,
        int totalSeen,
        List<BatchEntity> confirmedUpdates,
        long totalStallNanos
    ) {
        var stallMs = totalStallNanos / 1_000_000L;
        if (stallMs > 100) {
            log.warn(
                "Performance Alert: Parser stalled for {}ms waiting on Worker (Database/Hashing bottleneck)",
                stallMs
            );
        }

        var numTripUpdates = confirmedUpdates
            .stream()
            .filter(update -> update.type() == 3)
            .count();
        var numPositionUpdates = confirmedUpdates
            .stream()
            .filter(update -> update.type() == 4)
            .count();
        var numAlerts =
            confirmedUpdates.size() - numTripUpdates - numPositionUpdates;

        log.info(
            "Finished filtering {} from {} entities ({} trip updates, {} position updates, {} alerts)",
            SizeFormat.formatNumber(confirmedUpdates.size()),
            SizeFormat.formatNumber(totalSeen),
            SizeFormat.formatNumber(numTripUpdates),
            SizeFormat.formatNumber(numPositionUpdates),
            SizeFormat.formatNumber(numAlerts)
        );

        log.info(
            "Percentage of entities kept: {}%",
            totalSeen == 0
                ? 0
                : ((double) confirmedUpdates.size() / totalSeen) * 100
        );
    }
}
