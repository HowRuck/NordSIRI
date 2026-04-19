
CREATE TYPE IF NOT EXISTS feed_id_enum AS ENUM ('gtfs-de', 'entur');

CREATE TABLE IF NOT EXISTS trip_updates (
    id BIGINT NOT NULL,
    entity_id TEXT NOT NULL,
    feed_id feed_id_enum NOT NULL,
    trip_id TEXT,
    route_id TEXT,
    start_date DATE,
    start_time TIME,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_trip_updates_feed_id
    ON trip_updates (feed_id);

CREATE TYPE IF NOT EXISTS schedule_relationship_enum AS ENUM ('SCHEDULED', 'SKIPPED', 'NO_DATA');

CREATE TABLE IF NOT EXISTS trip_update_stop_time_updates (
    trip_update_id BIGINT NOT NULL REFERENCES trip_updates(id),
    feed_id feed_id_enum NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
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
    hash BIGINT NOT NULL,
    PRIMARY KEY (trip_update_id, received_at, stop_sequence)
);

SELECT create_hypertable(
    'trip_update_stop_time_updates',
    'received_at',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_trip_update_stop_time_updates_feed_received_at
    ON trip_update_stop_time_updates (feed_id, received_at DESC);
