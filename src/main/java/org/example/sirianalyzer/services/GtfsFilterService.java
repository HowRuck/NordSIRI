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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.services.filtering.GtfsFilterWorkerService;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterService {

    public static final int BATCH_SIZE = 10_000;
    public static final int ENTITY_FIELD_NUMBER = 2;

    // Sentinel object to tell the worker thread to stop.
    public static final ByteString POISON_PILL = ByteString.copyFromUtf8(
        "STOP"
    );

    private final MeterRegistry registry;
    private final GtfsFilterWorkerService workerService;

    /**
     * Represents a confirmed update to a GTFS entity
     */
    public record ConfirmedUpdate(int type, ByteString data) {}

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
    public List<ConfirmedUpdate> filter(String feedId, InputStream rawStream)
        throws IOException {
        var timerSample = Timer.start(registry);

        try {
            return filterInternal(feedId, rawStream);
        } finally {
            timerSample.stop(
                registry.timer("gtfs.filter.latency", "source", feedId)
            );
        }
    }

    /**
     * Internally filters a GTFS feed stream, returning a list of confirmed updates
     *
     * <p>
     * This method reads the raw GTFS feed stream, filters out entities that have
     * not changed since the last feed, and returns a list of confirmed updates.
     * </p>
     *
     * @param feedId The feed ID to associate with the updates
     * @param rawStream The raw GTFS feed stream to filter
     *
     * @return A list of confirmed updates
     *
     * @throws IOException If an error occurs while reading the stream
     */
    private List<ConfirmedUpdate> filterInternal(
        String feedId,
        InputStream rawStream
    ) throws IOException {
        var cis = CodedInputStream.newInstance(
            new BufferedInputStream(rawStream)
        );

        var confirmedUpdates = Collections.synchronizedList(
            new ArrayList<ConfirmedUpdate>()
        );

        var workQueue = new LinkedBlockingQueue<ByteString>(BATCH_SIZE * 2);
        var workerThread = Thread.ofVirtual().start(() ->
            workerService.runWorker(feedId, workQueue, confirmedUpdates)
        );

        var totalSeen = 0;
        var totalStallNanos = 0L;

        try {
            totalStallNanos = produceWork(cis, workQueue);
            totalSeen = lastSeenCount;
        } finally {
            workQueue.offer(POISON_PILL);
        }

        waitForWorker(workerThread);

        logSummary(feedId, totalSeen, confirmedUpdates, totalStallNanos);

        return confirmedUpdates;
    }

    /** Tracks the number of entities seen so far during the produceWork phase */
    private int lastSeenCount;

    /**
     * Reads entities from the CodedInputStream and adds them to the work queue
     *
     * @param cis The CodedInputStream to read from
     * @param workQueue The work queue to add entities to
     * @return The total stall time in nanoseconds
     *
     * @throws IOException If an error occurs while reading from the CodedInputStream
     */
    private long produceWork(
        CodedInputStream cis,
        BlockingQueue<ByteString> workQueue
    ) throws IOException {
        var seen = 0;
        var stallNanos = 0L;

        while (!cis.isAtEnd()) {
            var tag = cis.readTag();

            if (WireFormat.getTagFieldNumber(tag) != ENTITY_FIELD_NUMBER) {
                cis.skipField(tag);
                continue;
            }

            seen++;
            var data = cis.readBytes();
            var start = System.nanoTime();

            try {
                workQueue.put(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Producer interrupted", e);
                break;
            }

            stallNanos += System.nanoTime() - start;
        }

        lastSeenCount = seen;
        return stallNanos;
    }

    /**
     * Waits for the worker thread to complete
     *
     * @param workerThread the worker thread to wait for
     */
    private void waitForWorker(Thread workerThread) {
        try {
            workerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Worker interrupted", e);
        }
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
        List<ConfirmedUpdate> confirmedUpdates,
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
