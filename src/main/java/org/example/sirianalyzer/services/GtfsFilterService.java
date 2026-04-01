package org.example.sirianalyzer.services;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.model.GtfsByteEntity;
import org.example.sirianalyzer.proto.GtfsScanner;
import org.example.sirianalyzer.repositories.EntityHashRepository;
import org.example.sirianalyzer.util.FeedHashing;
import org.example.sirianalyzer.util.SizeFormat;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsFilterService {

    private final EntityHashRepository repository;
    private final MeterRegistry registry;

    // Cache to store the count of "updates" from the previous fetch per feed
    private final Map<String, Integer> lastUpdateCounts =
        new ConcurrentHashMap<>();
    private static final int DEFAULT_INITIAL_CAPACITY = 100;

    public List<GtfsByteEntity> filter(String feedId, InputStream rawStream)
        throws IOException {
        var sample = Timer.start(registry);

        var intialCapacity = lastUpdateCounts.getOrDefault(
            feedId,
            DEFAULT_INITIAL_CAPACITY
        );

        var cis = CodedInputStream.newInstance(
            new BufferedInputStream(rawStream)
        );

        var results = new ArrayList<GtfsByteEntity>(intialCapacity);
        var writeBuffer = new HashMap<String, Long>(intialCapacity);

        var totalSeen = 0;

        try (var readTxn = repository.openReadOnly()) {
            while (!cis.isAtEnd()) {
                var tag = cis.readTag();

                if (WireFormat.getTagFieldNumber(tag) == 2) {
                    totalSeen++;

                    var data = cis.readBytes();

                    var entityCis = data.newCodedInput();
                    var scan = GtfsScanner.scanEntity(entityCis);

                    if (scan.stableId() == null) continue;

                    var storageKey = feedId + ":" + scan.stableId();
                    var currentHash = FeedHashing.hashBytes(data);
                    var oldHash = repository.getHash(readTxn, storageKey);

                    if (oldHash == null || oldHash != currentHash) {
                        results.add(
                            GtfsByteEntity.of(
                                scan.stableId(),
                                scan.type(),
                                data
                            )
                        );
                        writeBuffer.put(storageKey, currentHash);
                    }
                } else {
                    cis.skipField(tag);
                }
            }
        }

        if (!results.isEmpty()) {
            lastUpdateCounts.put(feedId, results.size());
            repository.upsertBatch(feedId, writeBuffer);
        }

        log.info(
            "Filtered {} entities, totalSeen={}",
            SizeFormat.formatNumber(results.size()),
            SizeFormat.formatNumber(totalSeen)
        );

        sample.stop(registry.timer("gtfs.filter.latency", "source", feedId));
        registry.counter("gtfs.seen", "source", feedId).increment(totalSeen);
        registry
            .counter("gtfs.updated", "source", feedId)
            .increment(results.size());

        return results;
    }
}
