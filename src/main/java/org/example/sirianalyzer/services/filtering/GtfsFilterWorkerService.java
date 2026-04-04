package org.example.sirianalyzer.services.filtering;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.proto.GtfsScanner;
import org.example.sirianalyzer.repositories.EntityHashRepository;
import org.example.sirianalyzer.util.FeedHashing;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterWorkerService {

    private final EntityHashRepository repository;

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
    public BatchEntity processEntity(String feedId, ByteString data)
        throws IOException {
        var scan = GtfsScanner.scanEntity(data.newCodedInput());
        if (scan.stableId() == null) {
            return null;
        }

        var key = feedId + ":" + scan.stableId();
        var currentHash = FeedHashing.hashBytes(data);

        var entity = new BatchEntity(key, scan.type(), currentHash, data);

        return entity;
    }

    public List<BatchEntity> processBatch(
        String feedId,
        List<ByteString> data
    ) {
        log.info("Processing batch of {} entities", data.size());

        var pendingEntities = new ArrayList<BatchEntity>(data.size());

        for (var d : data) {
            try {
                var entity = processEntity(feedId, d);

                if (entity != null) {
                    pendingEntities.add(entity);
                }
            } catch (IOException e) {
                log.error("Failed to scan entity", e);
            }
        }

        return flushPendingBatch(pendingEntities);
    }

    /**
     * Flushes pending batch state.
     *
     * @param pendingMetas pending entity metadata
     * @param pendingPayloads pending payloads
     * @param confirmedUpdates confirmed updates
     */
    public List<BatchEntity> flushPendingBatch(
        List<BatchEntity> pendingEntities
    ) {
        if (pendingEntities.isEmpty()) {
            return List.of();
        }

        try {
            var keys = new ArrayList<String>(pendingEntities.size());

            for (var e : pendingEntities) {
                keys.add(e.key());
            }

            var existingHashes = repository.getHashBatch(keys);
            var redisUpdates = new HashMap<String, Long>(keys.size());
            var changedEntities = new ArrayList<BatchEntity>(
                pendingEntities.size()
            );

            for (var i = 0; i < pendingEntities.size(); i++) {
                var e = pendingEntities.get(i);

                var existingHash = existingHashes.get(i);

                var isChanged =
                    existingHash == null || e.hash() != existingHash;

                if (isChanged) {
                    // Entity is unchanged, remove from pending batch
                    changedEntities.add(e);
                }
            }

            for (var e : pendingEntities) {
                redisUpdates.put(e.key(), e.hash());
            }

            repository.upsertBatch("test", redisUpdates);

            return changedEntities;
        } catch (Exception e) {
            log.error("Failed to process batch", e);
        }

        return List.of();
    }

    public record BatchEntity(
        String key,
        int type,
        long hash,
        ByteString payload
    ) {}
}
