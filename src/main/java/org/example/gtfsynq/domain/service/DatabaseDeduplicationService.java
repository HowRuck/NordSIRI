package org.example.gtfsynq.domain.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.gtfsynq.domain.model.dto.TripStopTimeUpdateDto;
import org.example.gtfsynq.domain.model.dto.TripUpdateDto;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapHashStore;
import org.example.gtfsynq.infrastructure.protobuf.offheap.OffHeapLongTable;
import org.example.gtfsynq.util.FeedHashing;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DatabaseDeduplicationService {

    private final OffHeapHashStore stateStore;

    public TripUpdateDto cleanState(TripUpdateDto feedEntity) {
        var tripDescriptor = feedEntity.tripDescriptor();
        var tripKey = tripDescriptor.id();

        var tripHash = stateStore.get(tripKey);
        var tripDescriptorChanged = false;

        if (tripHash != tripDescriptor.hash()) {
            stateStore.put(tripKey, tripDescriptor.hash());
            tripDescriptorChanged = true;
        }

        var changedStopTimeUpdates = new ArrayList<TripStopTimeUpdateDto>(
            feedEntity.stopTimeUpdates().size()
        );

        for (var stopUpdate : feedEntity.stopTimeUpdates()) {
            var keyBytes = (tripKey + ":" + stopUpdate.stopSequence()).getBytes(
                StandardCharsets.UTF_8
            );
            var keyHash = FeedHashing.hashBytes(keyBytes);

            var currentHash = stopUpdate.hash();
            var existingHashData = stateStore.getWithCustomSlots(keyHash);
            var existingMetaHash = existingHashData[0];

            var currentArrivalDelay =
                stopUpdate.arrivalDelay() != null
                    ? stopUpdate.arrivalDelay()
                    : 0;
            var currentDepartureDelay =
                stopUpdate.departureDelay() != null
                    ? stopUpdate.departureDelay()
                    : 0;

            if (
                existingMetaHash == OffHeapLongTable.EMPTY_VALUE ||
                currentHash != existingMetaHash
            ) {
                changedStopTimeUpdates.add(stopUpdate);
                stateStore.put(
                    keyHash,
                    currentHash,
                    currentArrivalDelay,
                    currentDepartureDelay
                );
                continue;
            }

            var existingArrivalDelay = (int) existingHashData[1];
            var existingDepartureDelay = (int) existingHashData[2];

            var largestDelayDiff = Math.max(
                Math.abs(currentArrivalDelay - existingArrivalDelay),
                Math.abs(currentDepartureDelay - existingDepartureDelay)
            );

            if (largestDelayDiff > 60) {
                changedStopTimeUpdates.add(stopUpdate);
                stateStore.put(
                    keyHash,
                    currentHash,
                    currentArrivalDelay,
                    currentDepartureDelay
                );
            }
        }

        changedStopTimeUpdates.trimToSize();

        return new TripUpdateDto(
            tripDescriptorChanged ? tripDescriptor : null,
            changedStopTimeUpdates.isEmpty() ? null : changedStopTimeUpdates
        );
    }
}
