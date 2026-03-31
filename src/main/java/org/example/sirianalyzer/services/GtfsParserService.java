package org.example.sirianalyzer.services;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.sirianalyzer.model.GtfsByteFeedWrapper;
import org.example.sirianalyzer.proto.GtfsEntityType;
import org.example.sirianalyzer.proto.ProcessingAccumulator;
import org.example.sirianalyzer.proto.handler.FeedEntityHandler;
import org.example.sirianalyzer.repositories.GtfsStateRepository;
import org.example.sirianalyzer.util.ProtoUtils;
import org.example.sirianalyzer.util.SizeFormat;
import org.lmdbjava.Env;
import org.springframework.stereotype.Service;

/**
 * Orchestrates a full GTFS parse cycle: read phase (LMDB read txn + entity
 * dispatch), write phase (LMDB write txn), and wrapper assembly.
 *
 * <p>Adding a new entity type requires no changes here — create a new
 * {@link FeedEntityHandler} {@code @Component} with the correct
 * {@link FeedEntityHandler#fieldNumber()} and it is picked up automatically.</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsParserService {

    /** Field number of the repeated {@code FeedEntity} inside {@code FeedMessage}. */
    private static final int FEED_ENTITY_FIELD = 2;

    private final List<FeedEntityHandler> handlers;
    private final GtfsStateRepository repository;
    private final Env<ByteBuffer> env;
    private final MeterRegistry meterRegistry;

    /**
     * Direct-index dispatch table: {@code handlersByField[fieldNumber]} → handler.
     * Avoids boxing and hash-map overhead across ~250 k entity dispatches per cycle.
     */
    private FeedEntityHandler[] handlersByField;

    /** Capacity hint for the next run's {@link ProcessingAccumulator}. */
    private int lastRunChangedCount = 64;

    private Counter parseStartedCounter;
    private Counter parseFailedCounter;
    private Counter feedEntitiesCounter;
    private Timer parseTotalTimer;
    private Timer readPhaseTimer;
    private Timer entityDispatchTimer;
    private Timer payloadExtractTimer;
    private Timer handlerInvokeTimer;

    @PostConstruct
    public void init() {
        buildDispatchTable();
        initMetrics();
    }

    private void buildDispatchTable() {
        final var maxField = handlers
            .stream()
            .mapToInt(FeedEntityHandler::fieldNumber)
            .max()
            .orElse(0);

        handlersByField = new FeedEntityHandler[maxField + 1];

        for (var handler : handlers) {
            final var field = handler.fieldNumber();
            if (handlersByField[field] != null) {
                throw new IllegalStateException(
                    "Duplicate FeedEntityHandler for field " +
                        field +
                        ": " +
                        handlersByField[field].getClass().getSimpleName() +
                        " vs " +
                        handler.getClass().getSimpleName()
                );
            }
            handlersByField[field] = handler;
            log.info(
                "Registered FeedEntityHandler [{}] for FeedEntity field {}",
                handler.getClass().getSimpleName(),
                field
            );
        }
    }

    private void initMetrics() {
        parseStartedCounter = Counter.builder("gtfs.parse.started")
            .description("Number of GTFS parse operations started")
            .register(meterRegistry);

        parseFailedCounter = Counter.builder("gtfs.parse.failed")
            .description("Number of failed GTFS parse operations")
            .register(meterRegistry);

        feedEntitiesCounter = Counter.builder("gtfs.feed_entities.processed")
            .description("Number of FeedEntity messages scanned per parse run")
            .register(meterRegistry);

        parseTotalTimer = Timer.builder("gtfs.parse.total")
            .description("Wall-clock duration of the full GTFS parse cycle")
            .register(meterRegistry);

        readPhaseTimer = Timer.builder("gtfs.read.phase.total")
            .description("Wall-clock duration of the GTFS read phase")
            .register(meterRegistry);

        entityDispatchTimer = Timer.builder("gtfs.feed_entity.dispatch")
            .description(
                "Cumulative wall-clock time spent dispatching FeedEntity payloads"
            )
            .register(meterRegistry);

        payloadExtractTimer = Timer.builder("gtfs.feed_entity.payload_extract")
            .description(
                "Cumulative wall-clock time spent extracting handler payload bytes from FeedEntity messages"
            )
            .register(meterRegistry);

        handlerInvokeTimer = Timer.builder("gtfs.feed_entity.handler.invoke")
            .description(
                "Cumulative wall-clock time spent inside FeedEntityHandler.processPayload invocations"
            )
            .register(meterRegistry);
    }

    public GtfsByteFeedWrapper parseGtfs(InputStream feedByteStream) {
        parseStartedCounter.increment();

        final var totalStartNs = System.nanoTime();
        final var parseId = UUID.randomUUID().toString().substring(0, 8);

        log.info("[{}] Starting GTFS parse", parseId);

        for (var handler : handlers) {
            handler.beforeRun();
        }

        var startRead = System.nanoTime();
        final var acc = readPhase(feedByteStream, parseId, totalStartNs);
        if (acc == null) {
            log.error("[{}] Aborting: read phase failed", parseId);
            return null;
        }
        log.info(
            "[{}] Read phase completed in {} ms",
            parseId,
            (System.nanoTime() - startRead) / 1_000_000
        );

        log.info("[{}] Starting write phase", parseId);
        var startWrite = System.nanoTime();

        if (!writePhase(acc, parseId, totalStartNs)) {
            log.error("[{}] Aborting: write phase failed", parseId);
            return null;
        }

        log.info(
            "[{}] Write phase completed in {} ms",
            parseId,
            (System.nanoTime() - startWrite) / 1_000_000
        );

        final var totalNs = System.nanoTime() - totalStartNs;

        for (var handler : handlers) {
            handler.afterRun(totalNs);
        }

        feedEntitiesCounter.increment(acc.feedEntityCount);
        parseTotalTimer.record(totalNs, TimeUnit.NANOSECONDS);

        final var changedCount = acc.pendingUpdateCount();
        lastRunChangedCount = Math.max(changedCount, 16);

        log.info(
            "[{}] GTFS parse complete: {} FeedEntities, {} changed, total={} ms",
            parseId,
            SizeFormat.formatNumber(acc.feedEntityCount),
            SizeFormat.formatNumber(changedCount),
            TimeUnit.NANOSECONDS.toMillis(totalNs)
        );

        return new GtfsByteFeedWrapper(
            acc.collectChangedBytes(GtfsEntityType.TRIP_UPDATE),
            acc.collectChangedBytes(GtfsEntityType.VEHICLE_POSITION),
            acc.collectChangedBytes(GtfsEntityType.ALERT)
        );
    }

    private ProcessingAccumulator readPhase(
        InputStream feedByteStream,
        String parseId,
        long totalStartNs
    ) {
        final var readPhaseStartNs = System.nanoTime();
        final var stats = new ReadPhaseStats();

        try (var readTxn = env.txnRead()) {
            final var acc = new ProcessingAccumulator(
                readTxn,
                lastRunChangedCount
            );
            final var input = CodedInputStream.newInstance(feedByteStream);

            final var entityCount = ProtoUtils.forEachLengthDelimitedField(
                input,
                FEED_ENTITY_FIELD,
                entityBytes -> {
                    try {
                        dispatchEntity(entityBytes, acc, stats);
                    } catch (Exception e) {
                        log.error(
                            "[{}] Failed to process FeedEntity",
                            parseId,
                            e
                        );
                    }
                }
            );

            acc.feedEntityCount = entityCount;

            final var readPhaseNs = System.nanoTime() - readPhaseStartNs;
            readPhaseTimer.record(readPhaseNs, TimeUnit.NANOSECONDS);
            entityDispatchTimer.record(stats.dispatchNs, TimeUnit.NANOSECONDS);
            payloadExtractTimer.record(
                stats.payloadExtractNs,
                TimeUnit.NANOSECONDS
            );
            handlerInvokeTimer.record(
                stats.handlerInvokeNs,
                TimeUnit.NANOSECONDS
            );

            log.info(
                "[{}] Read phase breakdown: dispatch={} ms, payloadExtract={} ms, handlerInvoke={} ms",
                parseId,
                TimeUnit.NANOSECONDS.toMillis(stats.dispatchNs),
                TimeUnit.NANOSECONDS.toMillis(stats.payloadExtractNs),
                TimeUnit.NANOSECONDS.toMillis(stats.handlerInvokeNs)
            );

            return acc;
        } catch (Exception e) {
            parseFailedCounter.increment();
            log.error(
                "[{}] Read phase failed after {} ms",
                parseId,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalStartNs),
                e
            );
            return null;
        }
    }

    private void dispatchEntity(
        CodedInputStream entityStream,
        ProcessingAccumulator acc,
        ReadPhaseStats stats
    ) throws IOException {
        final var dispatchStartNs = System.nanoTime();

        try {
            while (!entityStream.isAtEnd()) {
                final var tag = entityStream.readTag();
                if (tag == 0) break;

                final var fieldNum = WireFormat.getTagFieldNumber(tag);
                final var wireType = WireFormat.getTagWireType(tag);

                if (
                    wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED &&
                    fieldNum < handlersByField.length &&
                    handlersByField[fieldNum] != null
                ) {
                    // TODO: Don't copy bytes but handle with CodedInputStream and limits

                    final var payloadStartNs = System.nanoTime();
                    final var payload = entityStream.readBytes();
                    stats.payloadExtractNs +=
                        System.nanoTime() - payloadStartNs;

                    final var handlerStartNs = System.nanoTime();
                    handlersByField[fieldNum].processPayload(payload, acc);
                    stats.handlerInvokeNs += System.nanoTime() - handlerStartNs;
                } else {
                    entityStream.skipField(tag);
                }
            }
        } finally {
            stats.dispatchNs += System.nanoTime() - dispatchStartNs;
        }
    }

    private static final class ReadPhaseStats {

        private long dispatchNs;
        private long payloadExtractNs;
        private long handlerInvokeNs;
    }

    private boolean writePhase(
        ProcessingAccumulator acc,
        String parseId,
        long totalStartNs
    ) {
        final var updates = acc.getPendingUpdates();
        log.info(
            "[{}] Writing {} changed entities to LMDB",
            parseId,
            updates.size()
        );

        try (var writeTxn = env.txnWrite()) {
            for (var update : updates) {
                for (var entry : update.lmdbWrites()) {
                    repository.putHash(writeTxn, entry.key(), entry.hash());
                }
            }
            writeTxn.commit();
            return true;
        } catch (Exception e) {
            parseFailedCounter.increment();
            log.error(
                "[{}] Write phase failed after {} ms",
                parseId,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalStartNs),
                e
            );
            return false;
        }
    }
}
