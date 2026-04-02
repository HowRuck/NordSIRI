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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.model.EntityHash;
import org.example.sirianalyzer.model.StorageKey;
import org.example.sirianalyzer.proto.GtfsScanner;
import org.example.sirianalyzer.repositories.EntityHashRepository;
import org.example.sirianalyzer.util.FeedHashing;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterService {

    private static final int BATCH_SIZE = 10_000;
    private static final int ENTITY_FIELD_NUMBER = 2;

    // Sentinel object to tell the worker thread to stop.
    private static final ByteString POISON_PILL = ByteString.copyFromUtf8(
        "STOP"
    );

    private final EntityHashRepository repository;
    private final MeterRegistry registry;

    /**
     * Represents metadata for an entity, including its storage key, type, and hash
     */
    private record EntityMeta(StorageKey key, int type, EntityHash hash) {}

    /**
     * Represents a confirmed update to a GTFS entity
     */
    public record ConfirmedUpdate(
        StorageKey key,
        int type,
        EntityHash hash,
        ByteString data
    ) {}

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
        // Wrap the raw stream in a CodedInputStream for efficient parsing
        var cis = CodedInputStream.newInstance(
            new BufferedInputStream(rawStream)
        );

        var confirmedUpdates = Collections.synchronizedList(
            new ArrayList<ConfirmedUpdate>()
        );

        // A blocking queue for work items (trip updates)
        var workQueue = new LinkedBlockingQueue<ByteString>(BATCH_SIZE * 2);
        // A virtual thread worker that processes work items from the queue
        var workerThread = Thread.ofVirtual().start(() ->
            runWorker(feedId, workQueue, confirmedUpdates)
        );

        // Counters for tracking progress and stall time
        var totalSeen = 0;
        var totalStallNanos = 0L;

        try {
            // Produce work by reading from the CodedInputStream and adding to the work queue
            totalStallNanos = produceWork(cis, workQueue);
            totalSeen = lastSeenCount;
        } finally {
            workQueue.offer(POISON_PILL);
        }

        waitForWorker(workerThread);

        writeConfirmedUpdates(feedId, confirmedUpdates);
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
            // Read the next tag from the stream and skip any fields that are not the entity field
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

            // Record the stall time for this entity to detech backpressure
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
     * Writes the confirmed updates to the repository
     *
     * @param feedId the feed ID to write updates for
     * @param confirmedUpdates the list of confirmed updates to write
     */
    private void writeConfirmedUpdates(
        String feedId,
        List<ConfirmedUpdate> confirmedUpdates
    ) {
        if (confirmedUpdates.isEmpty()) {
            return;
        }

        // Build a write buffer of key-hash pairs for efficient batch upsert
        var writeBuffer = new HashMap<StorageKey, EntityHash>(
            confirmedUpdates.size()
        );
        for (ConfirmedUpdate update : confirmedUpdates) {
            writeBuffer.put(update.key(), update.hash());
        }

        repository.upsertBatch(feedId, writeBuffer);
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

        // Calculate counts
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

    /**
     * Worker thread that processes entities from the queue and adds them to the results list
     *
     * @param feedId The feed ID associated with the queue
     * @param queue The queue from which entities are taken
     * @param results The list to which confirmed updates are added
     */
    private void runWorker(
        String feedId,
        BlockingQueue<ByteString> queue,
        List<ConfirmedUpdate> results
    ) {
        var pendingMetas = new ArrayList<EntityMeta>(BATCH_SIZE);
        var pendingPayloads = new ArrayList<ByteString>(BATCH_SIZE);

        try {
            // Process entities from the queue until a poison pill is encountered
            while (true) {
                var data = queue.take();
                if (data == POISON_PILL) {
                    break;
                }

                processEntity(
                    feedId,
                    data,
                    pendingMetas,
                    pendingPayloads,
                    results
                );
            }

            flushPendingBatch(pendingMetas, pendingPayloads, results);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Worker interrupted", e);
        } catch (IOException e) {
            log.error("Failed to scan entity", e);
        }
    }

    /**
     * Processes a single entity, adding its metadata and payload to the pending batch
     *
     * @param feedId The feed ID associated with the entity
     * @param data The entity data
     * @param pendingMetas The list to which entity metadata is added
     * @param pendingPayloads The list to which entity payload is added
     * @param results The list to which confirmed updates are added
     *
     * @throws IOException If an error occurs while scanning the entity
     */
    private void processEntity(
        String feedId,
        ByteString data,
        List<EntityMeta> pendingMetas,
        List<ByteString> pendingPayloads,
        List<ConfirmedUpdate> results
    ) throws IOException {
        // Scan the entity to get its stable ID and type
        var scan = GtfsScanner.scanEntity(data.newCodedInput());
        if (scan.stableId() == null) {
            return;
        }

        // Compute the storage key and hash of the entity
        var key = StorageKey.of(feedId, scan.stableId());
        var currentHash = FeedHashing.hashBytes(data);

        // Add the entity metadata and payload to the pending batch
        pendingMetas.add(new EntityMeta(key, scan.type(), currentHash));
        pendingPayloads.add(data);

        // If the pending batch reaches the batch size, flush it
        if (pendingMetas.size() >= BATCH_SIZE) {
            flushPendingBatch(pendingMetas, pendingPayloads, results);
        }
    }

    /**
     * Flushes the pending batch of entities to the repository
     *
     * @param pendingMetas Pending entity metadata
     * @param pendingPayloads Pending entity payloads
     * @param confirmedUpdates Confirmed updates to be returned to the caller
     */
    private void flushPendingBatch(
        List<EntityMeta> pendingMetas,
        List<ByteString> pendingPayloads,
        List<ConfirmedUpdate> confirmedUpdates
    ) {
        if (pendingMetas.isEmpty()) {
            return;
        }

        try {
            // Get the existing hashes for the pending batch from the repository
            var keys = pendingMetas.stream().map(EntityMeta::key).toList();
            var existingHashes = repository.getHashBatch(keys);

            // Compare each pending meta with its existing hash and add to confirmedUpdates if changed
            for (int i = 0; i < pendingMetas.size(); i++) {
                var meta = pendingMetas.get(i);
                var existingHash = existingHashes.get(i);

                var isChanged =
                    existingHash.value() == null ||
                    !existingHash.equals(meta.hash());

                if (isChanged) {
                    confirmedUpdates.add(
                        new ConfirmedUpdate(
                            meta.key(),
                            meta.type(),
                            meta.hash(),
                            pendingPayloads.get(i)
                        )
                    );
                }
            }
        } catch (Exception e) {
            log.error("Failed to process batch", e);
        } finally {
            // Clear the pending batch after processing
            pendingMetas.clear();
            pendingPayloads.clear();
        }
    }
}
