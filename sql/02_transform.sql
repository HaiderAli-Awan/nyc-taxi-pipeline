-- ============================================================
-- 02_transform.sql — raw.yellow_trips → clean.trips
-- Parameterized on :source_file so it's idempotent per month:
-- we delete that month's clean rows, then re-insert.
-- Run via: psql -v source_file="'yellow_tripdata_2024-01.parquet'" -f sql/02_transform.sql
-- (The ingestion script does this automatically.)
-- ============================================================

BEGIN;

-- Idempotency: wipe any previous clean rows for this file
DELETE FROM clean.trips WHERE source_file = :source_file;

INSERT INTO clean.trips (
    vendor_id, pickup_ts, dropoff_ts, duration_min,
    passenger_count, trip_distance_mi,
    pu_location_id, do_location_id, payment_type,
    fare_amount, tip_amount, total_amount,
    avg_speed_mph, source_file
)
SELECT
    vendor_id::SMALLINT,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    ROUND(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0, 2),
    -- NULL passenger counts are common; keep NULL rather than guessing
    NULLIF(passenger_count, 0)::SMALLINT,
    ROUND(trip_distance::NUMERIC, 2),
    pu_location_id::SMALLINT,
    do_location_id::SMALLINT,
    payment_type::SMALLINT,
    ROUND(fare_amount::NUMERIC, 2),
    ROUND(tip_amount::NUMERIC, 2),
    ROUND(total_amount::NUMERIC, 2),
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
        THEN ROUND(
            (trip_distance /
             (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0)
            )::NUMERIC, 2)
        ELSE NULL
    END,
    source_file
FROM raw.yellow_trips
WHERE source_file = :source_file
  -- ---- data quality gates (real-world messiness lives here) ----
  AND tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND tpep_dropoff_datetime > tpep_pickup_datetime          -- kills negative/zero durations
  AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) BETWEEN 60 AND 4 * 3600
                                                            -- 1 min to 4 hours
  AND trip_distance > 0
  AND trip_distance < 100                                   -- >100 mi in a yellow cab = GPS glitch
  AND pu_location_id BETWEEN 1 AND 265
  AND do_location_id BETWEEN 1 AND 265
  AND total_amount >= 0
  -- implied speed sanity: nothing legally drives 90+ mph through NYC
  AND (trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0)) < 90;

COMMIT;
