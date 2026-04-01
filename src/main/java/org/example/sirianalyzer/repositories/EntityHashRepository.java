package org.example.sirianalyzer.repositories;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class EntityHashRepository {

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final MeterRegistry registry;

    private final ThreadLocal<ByteBuffer> keyBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(512)
    );
    private final ThreadLocal<ByteBuffer> valueBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(Long.BYTES)
    );

    public Txn<ByteBuffer> openReadOnly() {
        return env.txnRead();
    }

    public Long getHash(Txn<ByteBuffer> txn, String stableId) {
        var key = writeToKeyBuffer(stableId);
        var found = db.get(txn, key);

        return (found == null) ? null : found.getLong();
    }

    public void upsertBatch(String source, Map<String, Long> batch) {
        if (batch.isEmpty()) return;

        var sample = Timer.start(registry);
        registry
            .counter("gtfs.lmdb.writes", "source", source)
            .increment(batch.size());

        try (var writeTxn = env.txnWrite()) {
            batch.forEach((id, hash) -> {
                var keyBuf = writeToKeyBuffer(id);
                var valueBuf = writeToValueBuffer(hash);

                db.put(writeTxn, keyBuf, valueBuf);
            });
            writeTxn.commit();

            sample.stop(
                registry.timer("gtfs.lmdb.commit.latency", "source", source)
            );
        }
    }

    private ByteBuffer writeToKeyBuffer(String stableId) {
        var buffer = keyBuffer.get();
        var keyBytes = stableId.getBytes();

        // Resize buffer if necessary
        if (keyBytes.length > buffer.capacity()) {
            buffer = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuffer.set(buffer);
        }

        buffer.clear();
        return buffer.put(keyBytes).flip();
    }

    private ByteBuffer writeToValueBuffer(long hash) {
        var buffer = valueBuffer.get();
        buffer.rewind();
        return buffer.putLong(hash).flip();
    }
}
