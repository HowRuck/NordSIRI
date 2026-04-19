package org.example.gtfsynq.domain.model.dto;

import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate.ScheduleRelationship;
import java.time.Instant;
import org.example.gtfsynq.domain.util.GtfsFeedFormatter;
import org.example.gtfsynq.util.FeedHashing;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record TripStopTimeUpdateDto(
    @NonNull long tripKey,
    @NonNull String feedId,
    @NonNull Instant receivedAt,
    @NonNull Integer stopSequence,
    @NonNull String stopId,
    @Nullable Instant arrivalTime,
    @Nullable Integer arrivalDelay,
    @Nullable Instant scheduledArrivalTime,
    @Nullable Instant departureTime,
    @Nullable Integer departureDelay,
    @Nullable Instant scheduledDepartureTime,
    @Nullable ScheduleRelationship scheduleRelationship,
    @Nullable String assignedStopId,
    long hash
) {
    public static TripStopTimeUpdateDto fromProto(
        long tripId,
        String feedId,
        Instant receivedAt,
        StopTimeUpdate stu
    ) {
        var arrival = stu.hasArrival() ? stu.getArrival() : null;
        var departure = stu.hasDeparture() ? stu.getDeparture() : null;

        var stopSequence = GtfsFeedFormatter.nullableInteger(
            stu.hasStopSequence(),
            stu.getStopSequence()
        );
        var stopId = GtfsFeedFormatter.nullableString(
            stu.hasStopId(),
            stu.getStopId()
        );

        Integer arrivalDelay = null;
        Instant arrivalTime = null;
        Instant scheduledArrivalTime = null;

        if (arrival != null) {
            arrivalDelay = GtfsFeedFormatter.nullableInteger(
                arrival.hasDelay(),
                arrival.getDelay()
            );

            arrivalTime = GtfsFeedFormatter.nullableInstant(
                arrival.hasTime(),
                arrival.getTime()
            );

            scheduledArrivalTime = GtfsFeedFormatter.nullableInstant(
                arrival.hasScheduledTime(),
                arrival.getScheduledTime()
            );
        }

        Integer departureDelay = null;
        Instant departureTime = null;
        Instant scheduledDepartureTime = null;

        if (departure != null) {
            departureDelay = GtfsFeedFormatter.nullableInteger(
                departure.hasDelay(),
                departure.getDelay()
            );

            departureTime = GtfsFeedFormatter.nullableInstant(
                departure.hasTime(),
                departure.getTime()
            );

            scheduledDepartureTime = GtfsFeedFormatter.nullableInstant(
                arrival.hasScheduledTime(),
                arrival.getScheduledTime()
            );
        }

        var scheduleRelationship = stu.getScheduleRelationship();
        var assignedStopId = GtfsFeedFormatter.nullableString(
            stu.hasStopId(),
            stu.getStopId()
        );

        var hash = hashPersistedFields(
            feedId,
            stopSequence,
            stopId,
            scheduledArrivalTime,
            scheduledDepartureTime,
            scheduleRelationship,
            assignedStopId
        );

        return new TripStopTimeUpdateDto(
            tripId,
            feedId,
            receivedAt,
            stopSequence,
            stopId,
            arrivalTime,
            arrivalDelay,
            scheduledArrivalTime,
            departureTime,
            departureDelay,
            scheduledDepartureTime,
            scheduleRelationship,
            assignedStopId,
            hash
        );
    }

    private static long hashPersistedFields(
        String feedId,
        Integer stopSequence,
        String stopId,
        Instant scheduledArrivalTime,
        Instant scheduledDepartureTime,
        ScheduleRelationship scheduleRelationship,
        String assignedStopId
    ) {
        return FeedHashing.encoder()
            .putInteger(feedId.hashCode())
            .putInteger(stopSequence)
            .putString(stopId)
            .putInstant(scheduledArrivalTime)
            .putInstant(scheduledDepartureTime)
            .putString(
                scheduleRelationship == null
                    ? null
                    : scheduleRelationship.name()
            )
            .putString(assignedStopId)
            .hash();
    }
}
