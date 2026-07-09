"""
NYC TLC Yellow Taxi ingestion pipeline.

Downloads monthly parquet files, validates the schema, loads them into
Postgres in chunks, and runs the raw -> clean transform.

Idempotency guarantee: each file is tracked in raw.load_registry.
- Already 'completed'  -> skipped entirely.
- 'failed'/'in_progress' (crashed run) -> its raw rows are deleted and
  the file is reloaded from scratch inside a transaction.
Running the script twice never duplicates data.

Usage:
    python ingest.py --months 2024-01 2024-02
    python ingest.py --months 2024-01 --force   # reload even if completed
"""

import argparse
import logging
import os
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import requests
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
log = logging.getLogger("ingest")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
CHUNK_ROWS = 100_000

# Expected columns in TLC yellow parquet -> our raw column names
EXPECTED_SCHEMA = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}


def db_url() -> str:
    return (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'taxi')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'taxi')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'taxi')}"
    )


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def download_month(month: str, dest_dir: Path) -> Path:
    """Download yellow_tripdata_{YYYY-MM}.parquet with streaming."""
    fname = f"yellow_tripdata_{month}.parquet"
    dest = dest_dir / fname
    if dest.exists():
        log.info("Already downloaded: %s", fname)
        return dest

    url = f"{BASE_URL}/{fname}"
    log.info("Downloading %s ...", url)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
        tmp.rename(dest)  # atomic: no half-written parquet ever gets used
    log.info("Saved %s (%.1f MB)", fname, dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_schema(parquet_path: Path) -> list[str]:
    """Assert the file contains every expected column. Returns ordered
    list of source column names present. Fails loudly on drift."""
    schema_names = set(pq.ParquetFile(parquet_path).schema_arrow.names)
    missing = [c for c in EXPECTED_SCHEMA if c not in schema_names]
    # airport_fee has changed capitalization across years — tolerate both
    if "Airport_fee" in missing and "airport_fee" in schema_names:
        missing.remove("Airport_fee")
    if missing:
        raise ValueError(f"Schema drift in {parquet_path.name}: missing {missing}")
    extra = schema_names - set(EXPECTED_SCHEMA) - {"airport_fee", "cbd_congestion_fee"}
    if extra:
        log.warning("Unexpected new columns (ignored): %s", sorted(extra))
    return [c for c in EXPECTED_SCHEMA if c in schema_names] + (
        ["airport_fee"] if "airport_fee" in schema_names and "Airport_fee" not in schema_names else []
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load_file(engine, parquet_path: Path, force: bool = False) -> None:
    source_file = parquet_path.name

    with engine.begin() as conn:
        status = conn.execute(
            text("SELECT status FROM raw.load_registry WHERE source_file = :f"),
            {"f": source_file},
        ).scalar()

    if status == "completed" and not force:
        log.info("SKIP %s — already loaded (idempotency).", source_file)
        return

    validate_schema(parquet_path)
    pf = pq.ParquetFile(parquet_path)
    total_rows = pf.metadata.num_rows
    log.info("Loading %s: %s rows in chunks of %s", source_file, f"{total_rows:,}", f"{CHUNK_ROWS:,}")

    # One transaction per file: either the whole month lands, or none of it.
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.load_registry (source_file, status)
                VALUES (:f, 'in_progress')
                ON CONFLICT (source_file)
                DO UPDATE SET status = 'in_progress', started_at = now(), completed_at = NULL
            """),
            {"f": source_file},
        )
        # Clear any partial rows from a previous crashed/forced run
        conn.execute(
            text("DELETE FROM raw.yellow_trips WHERE source_file = :f"),
            {"f": source_file},
        )

        loaded = 0
        for batch in pf.iter_batches(batch_size=CHUNK_ROWS):
            df = batch.to_pandas()
            # Normalize column names to our raw schema
            rename = {**EXPECTED_SCHEMA, "airport_fee": "airport_fee"}
            df = df.rename(columns=rename)
            df = df[[c for c in df.columns if c in set(EXPECTED_SCHEMA.values())]]
            df["source_file"] = source_file
            df.to_sql(
                "yellow_trips",
                conn,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )
            loaded += len(df)
            log.info("  ... %s / %s rows", f"{loaded:,}", f"{total_rows:,}")

        conn.execute(
            text("""
                UPDATE raw.load_registry
                SET status = 'completed', row_count = :n, completed_at = now()
                WHERE source_file = :f
            """),
            {"f": source_file, "n": loaded},
        )
    log.info("DONE %s (%s rows committed)", source_file, f"{loaded:,}")


def run_transform(engine, source_file: str) -> None:
    """Run raw -> clean transform for one file (delete + reinsert = idempotent)."""
    sql_path = Path(__file__).resolve().parent.parent / "sql" / "02_transform.sql"
    stmt = sql_path.read_text().replace(":source_file", ":sf")
    # Strip psql-style BEGIN/COMMIT; SQLAlchemy manages the transaction
    stmt = stmt.replace("BEGIN;", "").replace("COMMIT;", "")
    with engine.begin() as conn:
        conn.execute(text(stmt), {"sf": source_file})
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT COUNT(*) FROM clean.trips WHERE source_file = :sf"),
            {"sf": source_file},
        ).scalar()
    log.info("Transform complete: %s clean rows for %s", f"{n:,}", source_file)


def load_zones(engine) -> None:
    """One-time load of the taxi zone lookup table."""
    with engine.connect() as conn:
        if conn.execute(text("SELECT COUNT(*) FROM clean.zones")).scalar() > 0:
            log.info("Zones already loaded — skipping.")
            return
    import pandas as pd
    log.info("Downloading zone lookup ...")
    df = pd.read_csv(ZONE_LOOKUP_URL)
    df.columns = ["location_id", "borough", "zone", "service_zone"]
    with engine.begin() as conn:
        df.to_sql("zones", conn, schema="clean", if_exists="append", index=False)
    log.info("Loaded %d zones.", len(df))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="NYC TLC taxi ingestion")
    parser.add_argument("--months", nargs="+", required=True, help="e.g. 2024-01 2024-02")
    parser.add_argument("--force", action="store_true", help="Reload even if already completed")
    parser.add_argument("--data-dir", default=None, help="Where to cache parquet downloads")
    args = parser.parse_args()

    engine = create_engine(db_url(), pool_pre_ping=True)
    data_dir = Path(args.data_dir) if args.data_dir else Path(tempfile.gettempdir()) / "tlc_data"
    data_dir.mkdir(parents=True, exist_ok=True)

    load_zones(engine)

    for month in args.months:
        try:
            path = download_month(month, data_dir)
            source_file = path.name
            load_file(engine, path, force=args.force)
            run_transform(engine, source_file)
        except Exception:
            log.exception("FAILED month %s", month)
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE raw.load_registry SET status='failed' WHERE source_file=:f"),
                    {"f": f"yellow_tripdata_{month}.parquet"},
                )
            return 1
    log.info("All months processed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
