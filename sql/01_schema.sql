-- ============================================================
-- 01_schema.sql — Raw landing zone + cleaned zone + registry
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;

-- ------------------------------------------------------------
-- Load registry: the heart of idempotency.
-- Every source file we ingest gets exactly one row here.
-- If the file is already 'completed', the ingestion script skips it.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.load_registry (
    source_file   TEXT PRIMARY KEY,          -- e.g. yellow_tripdata_2024-01.parquet
    status        TEXT NOT NULL DEFAULT 'in_progress'
                  CHECK (status IN ('in_progress', 'completed', 'failed')),
    row_count     BIGINT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- Raw table: mirrors the TLC parquet schema, everything nullable.
-- We never clean here — raw means raw. source_file lets us
-- delete-and-reload a single month atomically (idempotency).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.yellow_trips (
    vendor_id               INT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    ratecode_id             DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    pu_location_id          INT,
    do_location_id          INT,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION,
    source_file             TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_trips_source_file
    ON raw.yellow_trips (source_file);

-- ------------------------------------------------------------
-- Cleaned table: typed, validated, deduplicated, enriched.
-- Populated by sql/02_transform.sql after each load.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.trips (
    trip_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vendor_id          SMALLINT,
    pickup_ts          TIMESTAMP NOT NULL,
    dropoff_ts         TIMESTAMP NOT NULL,
    duration_min       NUMERIC(8,2) NOT NULL,   -- derived
    passenger_count    SMALLINT,
    trip_distance_mi   NUMERIC(8,2) NOT NULL,
    pu_location_id     SMALLINT NOT NULL,
    do_location_id     SMALLINT NOT NULL,
    payment_type       SMALLINT,
    fare_amount        NUMERIC(10,2),
    tip_amount         NUMERIC(10,2),
    total_amount       NUMERIC(10,2),
    avg_speed_mph      NUMERIC(6,2),            -- derived
    source_file        TEXT NOT NULL,
    CONSTRAINT positive_duration CHECK (duration_min > 0)
);

CREATE INDEX IF NOT EXISTS idx_clean_pickup_ts ON clean.trips (pickup_ts);
CREATE INDEX IF NOT EXISTS idx_clean_pu_zone   ON clean.trips (pu_location_id);
CREATE INDEX IF NOT EXISTS idx_clean_source    ON clean.trips (source_file);

-- ------------------------------------------------------------
-- Zone lookup (loaded once from TLC's taxi_zone_lookup.csv)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.zones (
    location_id   SMALLINT PRIMARY KEY,
    borough       TEXT,
    zone          TEXT,
    service_zone  TEXT
);
