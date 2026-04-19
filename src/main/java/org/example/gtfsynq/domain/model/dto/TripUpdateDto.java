package org.example.gtfsynq.domain.model.dto;

import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public record TripUpdateDto(
    TripDescriptorDto tripDescriptor,
    List<TripStopTimeUpdateDto> stopTimeUpdates
) {
    public static TripUpdateDto fromEntity(
        FeedEntity entity,
        String feedId,
        Instant observedAt
    ) {
        if (!entity.hasTripUpdate()) return null;

        var update = entity.getTripUpdate();

        var tripDescriptor = TripDescriptorDto.fromEntity(
            update.getTrip(),
            feedId,
            entity.getId(),
            observedAt
        );

        var stopTimeUpdates = new ArrayList<TripStopTimeUpdateDto>(
            update.getStopTimeUpdateCount()
        );
        for (int index = 0; index < update.getStopTimeUpdateCount(); index++) {
            var stu = update.getStopTimeUpdate(index);

            stopTimeUpdates.add(
                TripStopTimeUpdateDto.fromProto(
                    tripDescriptor.id(),
                    feedId,
                    observedAt,
                    stu
                )
            );
        }

        return new TripUpdateDto(tripDescriptor, stopTimeUpdates);
    }
}
