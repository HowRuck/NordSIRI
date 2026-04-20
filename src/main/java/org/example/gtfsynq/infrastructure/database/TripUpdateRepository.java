package org.example.gtfsynq.infrastructure.database;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.example.gtfsynq.domain.model.dto.TripDescriptorDto;
import org.example.gtfsynq.domain.model.dto.TripStopTimeUpdateDto;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * High-throughput persistence for GTFS-RT TripUpdate entities.
 *
 * <p>This repository is designed for TimescaleDB-backed workloads:
 * the parent table is hypertable-friendly by using {@code received_at} as the time column,
 * while child tables remain normalized relational tables.
 */
@Repository
@RequiredArgsConstructor
public class TripUpdateRepository {

    private static final int BATCH_SIZE = 512;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Upserts the parent trip update row
     *
     * @param descriptors the list of trip descriptors to upsert
     */
    public void upsertTripDescriptors(List<TripDescriptorDto> descriptors) {
        if (descriptors == null || descriptors.isEmpty()) {
            return;
        }

        var currentTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);

        var sql = """
            INSERT INTO trip_updates (
                entity_id,
                feed_id,
                id,
                trip_id,
                route_id,
                start_date,
                start_time,
                start_time_overflow_days,
                received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                feed_id = EXCLUDED.feed_id,
                trip_id = EXCLUDED.trip_id,
                route_id = EXCLUDED.route_id,
                start_date = EXCLUDED.start_date,
                start_time = EXCLUDED.start_time,
                start_time_overflow_days = EXCLUDED.start_time_overflow_days,
                received_at = EXCLUDED.received_at
            """;

        jdbcTemplate.batchUpdate(
            sql,
            descriptors,
            BATCH_SIZE,
            (preparedStatement, descriptor) -> {
                preparedStatement.setString(1, descriptor.entityId());
                preparedStatement.setObject(
                    2,
                    descriptor.feedId(),
                    Types.OTHER
                );
                preparedStatement.setLong(3, descriptor.id());
                preparedStatement.setString(4, descriptor.tripId());
                preparedStatement.setString(5, descriptor.routeId());
                preparedStatement.setDate(
                    6,
                    descriptor.startDate() == null
                        ? null
                        : Date.valueOf(descriptor.startDate())
                );
                preparedStatement.setTime(
                    7,
                    descriptor.startTime() == null
                        ? null
                        : Time.valueOf(descriptor.startTime())
                );
                preparedStatement.setObject(
                    8,
                    descriptor.startTimeOverflowDays(),
                    Types.SMALLINT
                );
                preparedStatement.setTimestamp(9, Timestamp.from(currentTime));
            }
        );
    }

    /**
     * Appends a list of trip stop time updates to the database.
     *
     * @param updates the list of trip stop time updates to append
     */
    public void appendTripUpdates(List<TripStopTimeUpdateDto> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }

        var sql = """
            INSERT INTO trip_update_stop_time_updates (
                trip_update_id,
                feed_id,
                received_at,
                stop_sequence,
                stop_id,
                arrival_time,
                arrival_delay,
                scheduled_arrival_time,
                departure_time,
                departure_delay,
                scheduled_departure_time,
                schedule_relationship,
                assigned_stop_id,
                hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

        jdbcTemplate.batchUpdate(
            sql,
            updates,
            BATCH_SIZE,
            (preparedStatement, update) -> {
                preparedStatement.setLong(1, update.tripKey());
                preparedStatement.setObject(2, update.feedId(), Types.OTHER);
                preparedStatement.setTimestamp(
                    3,
                    Timestamp.from(update.receivedAt())
                );
                setNullableInteger(preparedStatement, 4, update.stopSequence());
                preparedStatement.setString(5, update.stopId());
                preparedStatement.setTimestamp(
                    6,
                    update.arrivalTime() == null
                        ? null
                        : Timestamp.from(update.arrivalTime())
                );
                setNullableInteger(preparedStatement, 7, update.arrivalDelay());
                preparedStatement.setTimestamp(
                    8,
                    update.scheduledArrivalTime() == null
                        ? null
                        : Timestamp.from(update.scheduledArrivalTime())
                );
                preparedStatement.setTimestamp(
                    9,
                    update.departureTime() == null
                        ? null
                        : Timestamp.from(update.departureTime())
                );
                setNullableInteger(
                    preparedStatement,
                    10,
                    update.departureDelay()
                );
                preparedStatement.setTimestamp(
                    11,
                    update.scheduledDepartureTime() == null
                        ? null
                        : Timestamp.from(update.scheduledDepartureTime())
                );
                preparedStatement.setObject(
                    12,
                    update.scheduleRelationship() == null
                        ? null
                        : update.scheduleRelationship(),
                    Types.OTHER
                );
                preparedStatement.setString(13, update.assignedStopId());
                preparedStatement.setLong(14, update.hash());
            }
        );
    }

    /**
     * Utility method to set a nullable integer value in a prepared statement
     *
     * @param ps the prepared statement
     * @param index the parameter index
     * @param value the integer value to set, or null for a null value
     * @throws SQLException if a database access error occurs
     */
    private void setNullableInteger(
        PreparedStatement ps,
        int index,
        Integer value
    ) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }
}
