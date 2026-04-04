package org.example.sirianalyzer.services.filtering;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.proto.GtfsScanner;
import org.example.sirianalyzer.repositories.EntityHashRepository;
import org.example.sirianalyzer.services.GtfsFilterService;
import org.example.sirianalyzer.util.FeedHashing;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterWorkerService {

    private final EntityHashRepository repository;

    /**
     * Runs the worker loop that consumes queued entities and appends confirmed updates.
     *
     * @param feedId the feed identifier
     * @param queue the queue of entity payloads
     * @param results the list that accumulates confirmed updates
     */
    public void runWorker(
        String feedId,
        BlockingQueue<ByteString> queue,
        List<GtfsFilterService.ConfirmedUpdate> results
    ) {
        var pendingEntities = new ArrayList<BatchEntity>(
            GtfsFilterService.BATCH_SIZE
        );

        try {
            while (true) {
                var data = queue.take();
                if (data == GtfsFilterService.POISON_PILL) {
                    break;
                }

                processEntity(feedId, data, pendingEntities, results);
            }

            flushPendingBatch(pendingEntities, results);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Worker interrupted", e);
        } catch (IOException e) {
            log.error("Failed to scan entity", e);
        }
    }

    /**
     * Processes a single entity payload and appends it to the pending batch.
     *
     * @param feedId the feed identifier
     * @param data the entity payload
     * @param pendingMetas pending entity metadata
     * @param pendingPayloads pending payloads
     * @param results confirmed updates
     * @throws IOException if scanning fails
     */
    public void processEntity(
        String feedId,
        ByteString data,
        List<BatchEntity> pendingEntities,
        List<GtfsFilterService.ConfirmedUpdate> results
    ) throws IOException {
        var scan = GtfsScanner.scanEntity(data.newCodedInput());
        if (scan.stableId() == null) {
            return;
        }

        var key = feedId + ":" + scan.stableId();
        var currentHash = FeedHashing.hashBytes(data);

        var entity = new BatchEntity(key, scan.type(), currentHash, data);
        pendingEntities.add(entity);

        if (pendingEntities.size() >= GtfsFilterService.BATCH_SIZE) {
            flushPendingBatch(pendingEntities, results);
        }
    }

    @Async
    public List<GtfsFilterService.ConfirmedUpdate> processBatch(
        String feedId,
        List<ByteString> data
    ) {
        log.info("Processing batch of {} entities", data.size());

        var pendingEntities = new ArrayList<BatchEntity>(data.size());
        var results = new ArrayList<GtfsFilterService.ConfirmedUpdate>(
            data.size()
        );

        for (var d : data) {
            try {
                processEntity(feedId, d, pendingEntities, results);
            } catch (IOException e) {
                log.error("Failed to scan entity", e);
            }
        }
        flushPendingBatch(pendingEntities, results);

        return results;
    }

    /**
     * Flushes pending batch state.
     *
     * @param pendingMetas pending entity metadata
     * @param pendingPayloads pending payloads
     * @param confirmedUpdates confirmed updates
     */
    public void flushPendingBatch(
        List<BatchEntity> pendingEntities,
        List<GtfsFilterService.ConfirmedUpdate> confirmedUpdates
    ) {
        if (pendingEntities.isEmpty()) {
            return;
        }

        try {
            var keys = new ArrayList<String>(pendingEntities.size());
            for (var e : pendingEntities) {
                keys.add(e.key());
            }

            var existingHashes = repository.getHashBatch(keys);
            var redisUpdates = new HashMap<String, Long>(keys.size());

            for (var i = 0; i < pendingEntities.size(); i++) {
                var e = pendingEntities.get(i);

                var existingHash = existingHashes.get(i);

                var isChanged =
                    existingHash == null || e.hash() != existingHash;

                if (isChanged) {
                    confirmedUpdates.add(
                        new GtfsFilterService.ConfirmedUpdate(
                            e.type(),
                            pendingEntities.get(i).payload()
                        )
                    );

                    redisUpdates.put(e.key(), e.hash());
                }
            }

            repository.upsertBatch("test", redisUpdates);
        } catch (Exception e) {
            log.error("Failed to process batch", e);
        } finally {
            pendingEntities.clear();
        }
    }

    private record BatchEntity(
        String key,
        int type,
        long hash,
        ByteString payload
    ) {}
}
