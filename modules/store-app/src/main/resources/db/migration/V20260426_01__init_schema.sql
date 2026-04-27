-- Enum for known GTFS feed identifiers.
CREATE TYPE feed_id_enum AS ENUM ('gtfs-de', 'entur', 'helsinki-hsl', 'ovapi', 'sncf');
-- Enum for stop-time schedule relationship values from GTFS-RT.
CREATE TYPE schedule_relationship_enum AS ENUM ('SCHEDULED', 'SKIPPED', 'NO_DATA', 'UNSCHEDULED', 'ADDED');

-- Stores one row per trip update entity with its extracted metadata.
CREATE TABLE rt_trip_updates_meta (
    id BIGINT NOT NULL,
    entity_id TEXT NOT NULL,
    feed_id feed_id_enum NOT NULL,
    trip_id TEXT,
    route_id TEXT,
    start_date DATE,
    start_time TIME,
    start_time_overflow_days SMALLINT,
    feed_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

-- Stores stop-time update rows in a TimescaleDB hypertable.
CREATE TABLE rt_stop_time_updates_ht (
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
    PRIMARY KEY (trip_update_id, feed_ts, stop_sequence)
);

-- Convert the stop-time table into a hypertable when it is created.
SELECT create_hypertable(
    'rt_stop_time_updates_ht',
    'feed_ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Speed up lookups by feed ID on trip updates.
CREATE INDEX idx_trip_updates_feed_id ON rt_trip_updates_meta (feed_id);

-- Optimize feed/time-range queries on stop-time updates.
CREATE INDEX idx_stop_updates_feed_ts
    ON rt_stop_time_updates_ht (feed_id, feed_ts DESC);

