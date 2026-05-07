-- Hot current-state tables for recently updated trip stop time data
-- These tables keep only the latest seen rows and can be cleaned up by a separate job
CREATE TABLE rt_trip_updates_hot (
    trip_update_id BIGINT NOT NULL,
    feed_id feed_id_enum NOT NULL,
    feed_ts TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trip_update_id)
);

CREATE INDEX idx_rt_trip_updates_hot_last_seen_at
    ON rt_trip_updates_hot (last_seen_at);

CREATE TABLE rt_stop_time_updates_hot (
    trip_update_id BIGINT NOT NULL,
    feed_id feed_id_enum NOT NULL,
    feed_ts TIMESTAMPTZ NOT NULL,
    stop_sequence INT NOT NULL,
    stop_id TEXT,
    arrival_time TIMESTAMPTZ,
    arrival_delay INT,
    scheduled_arrival_time TIMESTAMPTZ,
    departure_time TIMESTAMPTZ,
    departure_delay INT,
    scheduled_departure_time TIMESTAMPTZ,
    schedule_relationship schedule_relationship_enum,
    assigned_stop_id TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trip_update_id, stop_sequence),
    CONSTRAINT fk_rt_stop_time_updates_hot_trip_update
        FOREIGN KEY (trip_update_id)
        REFERENCES rt_trip_updates_hot (trip_update_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_rt_stop_time_updates_hot_last_seen_at
    ON rt_stop_time_updates_hot (last_seen_at);
