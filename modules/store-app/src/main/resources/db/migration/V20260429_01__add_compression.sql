-- Add compression policy to the rt_stop_time_updates_ht hypertable.
-- Compress chunks after 1 day to save disk space.
SELECT add_compression_policy('rt_stop_time_updates_ht', INTERVAL '1 day');
