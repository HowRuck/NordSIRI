package org.example.sirianalyzer.services.filtering;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.model.EntityHash;
import org.example.sirianalyzer.model.StorageKey;
import org.example.sirianalyzer.proto.GtfsScanner;
import org.example.sirianalyzer.repositories.EntityHashRepository;
import org.example.sirianalyzer.services.GtfsFilterService;
import org.example.sirianalyzer.util.FeedHashing;
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
        var pendingMetas = new ArrayList<EntityMeta>(
            GtfsFilterService.BATCH_SIZE
        );
        var pendingPayloads = new ArrayList<ByteString>(
            GtfsFilterService.BATCH_SIZE
        );

        try {
            while (true) {
                var data = queue.take();
                if (data == GtfsFilterService.POISON_PILL) {
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
        List<EntityMeta> pendingMetas,
        List<ByteString> pendingPayloads,
        List<GtfsFilterService.ConfirmedUpdate> results
    ) throws IOException {
        var scan = GtfsScanner.scanEntity(data.newCodedInput());
        if (scan.stableId() == null) {
            return;
        }

        var key = feedId + ":" + scan.stableId();
        var currentHash = FeedHashing.hashBytes(data);

        pendingMetas.add(new EntityMeta(key, scan.type(), currentHash));
        pendingPayloads.add(data);

        if (pendingMetas.size() >= GtfsFilterService.BATCH_SIZE) {
            flushPendingBatch(pendingMetas, pendingPayloads, results);
        }
    }

    /**
     * Flushes pending batch state.
     *
     * @param pendingMetas pending entity metadata
     * @param pendingPayloads pending payloads
     * @param confirmedUpdates confirmed updates
     */
    public void flushPendingBatch(
        List<EntityMeta> pendingMetas,
        List<ByteString> pendingPayloads,
        List<GtfsFilterService.ConfirmedUpdate> confirmedUpdates
    ) {
        if (pendingMetas.isEmpty()) {
            return;
        }

        try {
            var keys = new ArrayList<String>(pendingMetas.size());
            for (var meta : pendingMetas) {
                keys.add(meta.key());
            }

            var existingHashes = repository.getHashBatch(keys);
            var redisUpdates = new HashMap<String, Long>(keys.size());

            for (var i = 0; i < pendingMetas.size(); i++) {
                var meta = pendingMetas.get(i);

                var existingHash = existingHashes.get(i);

                var isChanged =
                    existingHash == null || meta.hash() != existingHash;

                if (isChanged) {
                    confirmedUpdates.add(
                        new GtfsFilterService.ConfirmedUpdate(
                            meta.type(),
                            pendingPayloads.get(i)
                        )
                    );

                    redisUpdates.put(meta.key(), meta.hash());
                }
            }

            repository.upsertBatch("test", redisUpdates);
        } catch (Exception e) {
            log.error("Failed to process batch", e);
        } finally {
            pendingMetas.clear();
            pendingPayloads.clear();
        }
    }

    private record EntityMeta(String key, int type, long hash) {}
}
