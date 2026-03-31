package org.example.sirianalyzer.proto.handler;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.example.sirianalyzer.proto.GtfsEntityType;
import org.example.sirianalyzer.proto.ProcessingAccumulator;
import org.example.sirianalyzer.proto.dedup.EntityUpdateFilter;
import org.springframework.stereotype.Component;

/**
 * Handles {@code TripUpdate} payloads extracted from {@code FeedEntity} field 3
 * <br/>
 * Delegates deduplication to the injected {@link EntityUpdateFilter}
 */
@Component
@RequiredArgsConstructor
public class TripUpdateHandler implements FeedEntityHandler {

    private static final int FIELD_NUMBER = 3;

    private final EntityUpdateFilter deduplicator;
    private final MeterRegistry meterRegistry;

    private int processedCount;
    private int changedCount;
    private long totalProcessNs;

    private Counter processedCounter;
    private Counter changedCounter;
    private Timer processTimer;

    @PostConstruct
    public void initMetrics() {
        processedCounter = Counter.builder("gtfs.trip_updates.processed")
            .description("TripUpdate payloads inspected per parse run")
            .register(meterRegistry);

        changedCounter = Counter.builder("gtfs.trip_updates.changed")
            .description(
                "TripUpdate payloads whose content changed since the last run"
            )
            .register(meterRegistry);

        processTimer = Timer.builder("gtfs.trip_update.process")
            .description(
                "Cumulative CPU time spent processing TripUpdate payloads per run"
            )
            .register(meterRegistry);
    }

    @Override
    public int fieldNumber() {
        return FIELD_NUMBER;
    }

    @Override
    public GtfsEntityType entityType() {
        return GtfsEntityType.TRIP_UPDATE;
    }

    @Override
    public void beforeRun() {
        processedCount = 0;
        changedCount = 0;
        totalProcessNs = 0;
    }

    @Override
    public void processPayload(ByteString payload, ProcessingAccumulator acc)
        throws IOException {
        processedCount++;

        final var start = System.nanoTime();

        var pendingUpdate = deduplicator.getPendingUpdateIfChanged(
            payload,
            acc.readTxn
        );
        if (pendingUpdate != null) {
            acc.addUpdate(pendingUpdate);
            changedCount++;
        }

        totalProcessNs += System.nanoTime() - start;
    }

    @Override
    public void afterRun(long runDurationNs) {
        processedCounter.increment(processedCount);
        changedCounter.increment(changedCount);
        processTimer.record(totalProcessNs, TimeUnit.NANOSECONDS);
    }
}
