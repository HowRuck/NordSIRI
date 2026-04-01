package org.example.sirianalyzer.repositories;

import com.google.common.primitives.Longs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private final RocksDB rocksDB;
    private final MeterRegistry registry;

    public Long getHash(String stableId) {
        try {
            var key = stableId.getBytes();
            var found = rocksDB.get(key);

            return (found == null) ? null : Longs.fromByteArray(found);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void upsertBatch(String source, Map<String, Long> batch) {
        if (batch.isEmpty()) return;

        var sample = Timer.start(registry);
        registry
            .counter("gtfs.lmdb.writes", "source", source)
            .increment(batch.size());

        try (var writeBatch = new WriteBatch()) {
            var writeOpts = new WriteOptions();

            for (var entry : batch.entrySet()) {
                var key = entry.getKey().getBytes();
                var hash = entry.getValue();

                writeBatch.put(key, Longs.toByteArray(hash));
            }

            rocksDB.write(writeOpts, writeBatch);
        } catch (RocksDBException e) {
            throw new RuntimeException("RocksDB batch write failed", e);
        }

        sample.stop(
            registry.timer("gtfs.lmdb.commit.latency", "source", source)
        );
    }
}
