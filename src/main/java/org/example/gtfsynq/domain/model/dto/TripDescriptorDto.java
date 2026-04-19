package org.example.gtfsynq.domain.model.dto;

import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import org.example.gtfsynq.domain.util.GtfsFeedFormatter;
import org.example.gtfsynq.util.FeedHashing;

public record TripDescriptorDto(
    long id,
    String entityId,
    String feedId,
    Instant observedAt,
    String tripId,
    String routeId,
    Integer directionId,
    LocalDate startDate,
    LocalTime startTime,
    long hash
    //ScheduleRelationship scheduleRelationship,
    //Integer stopTimeUpdateCount,
    //boolean isDeleted
) {
    public static TripDescriptorDto fromEntity(
        TripDescriptor tripDescriptor,
        String feedId,
        String entityId,
        Instant observedAt
    ) {
        if (tripDescriptor == null) return null;

        var hash = FeedHashing.hashBytes(tripDescriptor.toByteArray());

        var tripId = GtfsFeedFormatter.nullableString(
            tripDescriptor.hasTripId(),
            tripDescriptor.getTripId()
        );
        var routeId = GtfsFeedFormatter.nullableString(
            tripDescriptor.hasRouteId(),
            tripDescriptor.getRouteId()
        );
        var startDate = GtfsFeedFormatter.nullableDate(
            tripDescriptor.hasStartDate(),
            tripDescriptor.getStartDate()
        );
        var startTime = GtfsFeedFormatter.nullableTime(
            tripDescriptor.hasStartTime(),
            tripDescriptor.getStartTime()
        );
        var directionId = GtfsFeedFormatter.nullableInteger(
            tripDescriptor.hasDirectionId(),
            tripDescriptor.getDirectionId()
        );
        var tripKeyBytes = GtfsFeedFormatter.buildKey(
            feedId == null ? "" : feedId,
            entityId == null ? "" : entityId,
            routeId == null ? "" : routeId,
            startDate == null ? "" : startDate.toString(),
            startTime == null ? "" : startTime.toString()
        ).getBytes();

        var tripKeyBytesHash = FeedHashing.hashBytes(tripKeyBytes);

        return new TripDescriptorDto(
            tripKeyBytesHash,
            entityId,
            feedId,
            observedAt,
            tripId,
            routeId,
            directionId,
            startDate,
            startTime,
            hash
        );
    }
}
