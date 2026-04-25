package org.example.gtfsynq.application.service;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.domain.model.dto.TripUpdateDto;
import org.example.gtfsynq.domain.service.DatabaseDeduplicationService;
import org.example.gtfsynq.infrastructure.database.TripUpdateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Buffers GTFS TripUpdate writes and flushes them to the database in batches.
 *
 * <p>This sink is intended for high-throughput ingestion where individual message writes would be
 * too expensive. Incoming updates are coalesced by entity id so that the latest update wins before
 * being written to the database.
 *
 * <p>The sink keeps only the most recent update per entity id in memory. On flush, it persists the
 * parent trip-update row and all normalized child rows in one transactional operation per entity.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GtfsTripUpdateSink {

    private final TripUpdateRepository tripUpdateRepository;

    private final ReentrantLock bufferLock = new ReentrantLock();
    private final List<TripUpdateDto> buffer = new LinkedList<>();

    private final DatabaseDeduplicationService deduplicationService;

    @Value("${gtfs.sink.enabled:true}")
    private boolean enabled;

    /**
     * Accepts a feed entity and buffers it for later batch persistence.
     *
     * @param feedId feed identifier from Kafka key
     * @param entity decoded GTFS-RT feed entity
     */
    public void accept(String feedId, FeedEntity entity) {
        if (!enabled || entity == null || !entity.hasTripUpdate()) {
            return;
        }

        var currentTime = Instant.now();
        var rawUpdateDto = TripUpdateDto.fromEntity(
            entity,
            feedId,
            currentTime
        );

        bufferLock.lock();
        try {
            var cleanedUpdate = deduplicationService.cleanState(rawUpdateDto);
            buffer.add(cleanedUpdate);

            log.debug(
                "Buffered TripUpdate entity={} feed={} bufferSize={}",
                cleanedUpdate.tripDescriptor() != null
                    ? cleanedUpdate.tripDescriptor().entityId()
                    : null,
                feedId,
                buffer.size()
            );
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * Flushes the current buffer on a schedule.
     */
    @Scheduled(fixedDelayString = "${gtfs.sink.flush-interval-ms:10000}")
    public void scheduledFlush() {
        if (!enabled) {
            return;
        }

        bufferLock.lock();
        try {
            flushBufferLocked();
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * Flushes any buffered updates immediately.
     */
    public void flushNow() {
        if (!enabled) {
            return;
        }

        bufferLock.lock();
        try {
            log.info(
                "Manual flush requested for {} buffered TripUpdate records",
                buffer.size()
            );
            flushBufferLocked();
        } finally {
            bufferLock.unlock();
        }
    }

    private void flushBufferLocked() {
        if (buffer.isEmpty()) {
            return;
        }

        var flushSize = buffer.size();

        var tripDescriptors = buffer
            .stream()
            .map(TripUpdateDto::tripDescriptor)
            .filter(Objects::nonNull)
            .toList();

        var stopTimeUpdates = buffer
            .stream()
            .map(TripUpdateDto::stopTimeUpdates)
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .filter(u -> u.stopSequence() != null)
            .toList();

        tripUpdateRepository.upsertTripDescriptors(tripDescriptors);
        tripUpdateRepository.appendTripUpdates(stopTimeUpdates);

        buffer.clear();

        log.info(
            "Flushed {} buffered TripUpdate records ({} descriptors, {} stop-time updates)",
            flushSize,
            tripDescriptors.size(),
            stopTimeUpdates.size()
        );
    }
}
