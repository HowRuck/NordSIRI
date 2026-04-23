-- 1. Drop existing objects in reverse order of dependency
-- CASCADE handles the removal of indices and associated views
DROP TABLE IF EXISTS trip_update_stop_time_updates CASCADE;
DROP TABLE IF EXISTS trip_updates CASCADE;

-- Drop Enums (CASCADE here ensures any old columns using them are cleared)
DROP TYPE IF EXISTS feed_id_enum CASCADE;
DROP TYPE IF EXISTS schedule_relationship_enum CASCADE;

-- 2. Recreate Enums
CREATE TYPE feed_id_enum AS ENUM ('gtfs-de', 'entur', 'helsinki-hsl', 'ovapi');
CREATE TYPE schedule_relationship_enum AS ENUM ('SCHEDULED', 'SKIPPED', 'NO_DATA', 'UNSCHEDULED', 'ADDED');

-- 3. Recreate Parent Table
CREATE TABLE trip_updates (
    id BIGINT NOT NULL,
    entity_id TEXT NOT NULL,
    feed_id feed_id_enum NOT NULL,
    trip_id TEXT,
    route_id TEXT,
    start_date DATE,
    start_time TIME,
    start_time_overflow_days SMALLINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

-- 4. Recreate Child Table (Hypertables cannot have traditional Foreign Keys to non-hypertables easily)
-- We remove the REFERENCES constraint for the hypertable to ensure maximum compatibility with partitioning
CREATE TABLE trip_update_stop_time_updates (
    trip_update_id BIGINT NOT NULL,
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

-- 5. Convert to Hypertable
-- We use create_hypertable. Since we drop at start, migrate_data is unnecessary.
SELECT create_hypertable(
    'trip_update_stop_time_updates',
    'received_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 6. Recreate Indices
CREATE INDEX idx_trip_updates_feed_id ON trip_updates (feed_id);

CREATE INDEX idx_stop_updates_feed_received
    ON trip_update_stop_time_updates (feed_id, received_at DESC);
