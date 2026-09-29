"""
bronze_ingestion_pipeline.py

Loads raw Parquet files from the MinIO/S3 data lake (Raw Bronze layer) into the
Postgres analytics database's `bronze` schema.

Three sources of truth drive the whole pipeline:

  1. SOURCES        - which bucket prefix each bronze table reads from, and which
                       loading pattern (strategy) applies to it.
  2. BRONZE_DDL      - the explicit CREATE TABLE statements (loosely typed, no
                       constraints - bronze just needs to land the data faithfully).
  3. bronze._processed_files - a control table that makes incremental loads
                       idempotent (safe to re-run without duplicating rows).

Three loading patterns, one per source shape:

  - snapshot_tagged     : exactly one file, always named `_latest.parquet`
                           -> truncate the table, reload that one file
  - snapshot_partition  : a full dump under a dated `snapshot_date=.../` folder
                           -> truncate the table, reload the newest partition
  - incremental          : many append-only files, one per extraction run
                           -> load only files not yet recorded in the control table

Usage:
    python bronze_ingestion_pipeline.py
    python bronze_ingestion_pipeline.py --tables bronze.pg_orders bronze.pg_customers
    python bronze_ingestion_pipeline.py --log-level DEBUG --log-file pipeline.log
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from typing import Optional

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Make config_loader.py (one directory up) importable, same convention as the notebook.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config_loader import CONFIG


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

LOGGER_ROOT_NAME = "bronze_pipeline"


def setup_logger(level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure the app's root logger ("bronze_pipeline") once.

    Every class in this module logs through a child logger
    (e.g. "bronze_pipeline.reader"), which inherits this configuration by
    propagating up to this logger - so all output goes through the same
    handlers/format without every class needing its own setup.
    """
    logger = logging.getLogger(LOGGER_ROOT_NAME)
    logger.setLevel(level.upper())
    logger.propagate = False  # don't also hand records to the root/default logger

    logger.handlers.clear()  # safe to call setup_logger() more than once (e.g. in tests)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# --------------------------------------------------------------------------- #
# Source configuration - one entry per bronze table
# --------------------------------------------------------------------------- #

SOURCES: dict[str, dict[str, str]] = {
    "bronze.api_geolocation": {
        "pattern": "snapshot_tagged",
        "prefix": "api/api_endpoint=geolocation/",
    },
    "bronze.api_translations": {
        "pattern": "snapshot_tagged",
        "prefix": "api/api_endpoint=translations/",
    },
    "bronze.pg_customers": {
        "pattern": "snapshot_partition",
        "prefix": "postgres/snapshots/table_name=customers/",
    },
    "bronze.pg_products": {
        "pattern": "snapshot_partition",
        "prefix": "postgres/snapshots/table_name=products/",
    },
    "bronze.pg_sellers": {
        "pattern": "snapshot_partition",
        "prefix": "postgres/snapshots/table_name=sellers/",
    },
    "bronze.pg_orders": {
        "pattern": "incremental",
        "prefix": "postgres/batches/table_name=orders/",
    },
    "bronze.pg_order_items": {
        "pattern": "incremental",
        "prefix": "postgres/batches/table_name=order_items/",
    },
    "bronze.stream_order_payments": {
        "pattern": "incremental",
        "prefix": "external_s3/stream_event=order_payments/",
    },
    "bronze.stream_order_reviews": {
        "pattern": "incremental",
        "prefix": "external_s3/stream_event=order_reviews/",
    },
}


BRONZE_DDL = """
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.api_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(2),
    _source_file                TEXT,
    _ingested_at                TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.api_translations (
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100),
    _source_file                   TEXT,
    _ingested_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.stream_order_payments (
    order_id             VARCHAR(64),
    payment_sequential   INTEGER,
    payment_type         VARCHAR(30),
    payment_installments INTEGER,
    payment_value        NUMERIC(12,2),
    _source_file         TEXT,
    _ingested_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.stream_order_reviews (
    review_id               VARCHAR(64),
    order_id                VARCHAR(64),
    review_score            SMALLINT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _source_file            TEXT,
    _ingested_at            TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_orders (
    order_id                       VARCHAR(64),
    customer_id                    VARCHAR(64),
    order_status                   VARCHAR(20),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    _source_file                   TEXT,
    _ingested_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_order_items (
    order_id             VARCHAR(64),
    order_item_id        INTEGER,
    product_id           VARCHAR(64),
    seller_id            VARCHAR(64),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(12,2),
    freight_value        NUMERIC(12,2),
    _source_file         TEXT,
    _ingested_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_customers (
    customer_id               VARCHAR(64),
    customer_unique_id        VARCHAR(64),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            VARCHAR(2),
    _source_file              TEXT,
    _ingested_at              TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_products (
    product_id                   VARCHAR(64),
    product_category_name        VARCHAR(100),
    product_name_lenght          NUMERIC,
    product_description_lenght   NUMERIC,
    product_photos_qty           NUMERIC,
    product_weight_g             NUMERIC(10,2),
    product_length_cm            NUMERIC(10,2),
    product_height_cm            NUMERIC(10,2),
    product_width_cm             NUMERIC(10,2),
    _source_file                 TEXT,
    _ingested_at                 TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_sellers (
    seller_id               VARCHAR(64),
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(2),
    _source_file            TEXT,
    _ingested_at            TIMESTAMP DEFAULT now()
);

-- Control table: makes incremental (Pattern B) loads idempotent. For every file
-- successfully loaded we record (table_name, file_key); on the next run we skip
-- any file_key already present here, so re-running the pipeline never re-inserts
-- the same rows twice.
CREATE TABLE IF NOT EXISTS bronze._processed_files (
    table_name  VARCHAR(100) NOT NULL,
    file_key    TEXT NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT now(),
    row_count   INTEGER,
    PRIMARY KEY (table_name, file_key)
);
"""


# --------------------------------------------------------------------------- #
# Data lake reader - everything that talks to S3 / MinIO
# --------------------------------------------------------------------------- #

class DataLakeReader:
    """Reads Parquet files and file listings from the S3-compatible data lake."""

    def __init__(self, s3_client, bucket: str):
        self.s3 = s3_client
        self.bucket = bucket
        self.log = logging.getLogger(f"{LOGGER_ROOT_NAME}.reader")

    def list_files_with_meta(self, prefix: str) -> list[tuple[str, str]]:
        """Return (key, last_modified_isoformat) for every .parquet file under `prefix`."""
        paginator = self.s3.get_paginator("list_objects_v2")
        files: list[tuple[str, str]] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    files.append((obj["Key"], obj["LastModified"].isoformat()))
        self.log.debug("Listed %d parquet file(s) under prefix '%s'", len(files), prefix)
        return files

    def get_latest_tagged_file(self, prefix: str) -> tuple[str, str]:
        """Return the single file under `prefix` whose name ends in `_latest.parquet`.

        Used for Pattern A / 'snapshot_tagged' sources (geolocation, translations).
        """
        files = self.list_files_with_meta(prefix)
        tagged = [(key, ts) for key, ts in files if key.endswith("_latest.parquet")]
        if not tagged:
            raise FileNotFoundError(f"No '_latest.parquet' file found under prefix '{prefix}'")
        return tagged[0]

    def get_latest_snapshot_partition_files(self, prefix: str) -> tuple[str, str]:
        """Return the most recently modified file under `prefix`.

        Used for Pattern A / 'snapshot_partition' sources (customers, products,
        sellers). Picking the max LastModified across all files under the prefix
        is equivalent to picking the newest snapshot_date partition, without
        needing to parse dates out of the path.
        """
        files = self.list_files_with_meta(prefix)
        if not files:
            raise FileNotFoundError(f"No parquet files found under prefix '{prefix}'")
        return max(files, key=lambda item: item[1])

    def read_keys_to_df(self, keys: list[str]) -> pd.DataFrame:
        """Download and concatenate the given S3 keys into a single DataFrame.

        Stamps a `_source_file` column onto every row so any row can be traced
        back to the exact file it came from.
        """
        frames = []
        for key in keys:
            body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            df = pd.read_parquet(io.BytesIO(body), engine="pyarrow")
            df["_source_file"] = key
            frames.append(df)
            self.log.debug("Read %d row(s) from '%s'", len(df), key)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --------------------------------------------------------------------------- #
# Postgres warehouse loader - everything that talks to Postgres
# --------------------------------------------------------------------------- #

class PostgresWarehouseLoader:
    """Handles DDL setup and bulk loading into the Postgres `bronze` schema."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.log = logging.getLogger(f"{LOGGER_ROOT_NAME}.loader")

    def run_sql(self, sql: str) -> None:
        """Execute an arbitrary multi-statement SQL script in one transaction."""
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def create_bronze_schema(self) -> None:
        """Create the bronze schema, all bronze tables, and the control table."""
        self.log.info("Ensuring bronze schema and tables exist")
        self.run_sql(BRONZE_DDL)

    def load_df(self, df: pd.DataFrame, table_name: str, truncate: bool = True) -> int:
        """Bulk-load a DataFrame into `table_name` ('schema.table') via Postgres COPY.

        Uses COPY (not INSERT) because it streams rows straight into table
        storage without per-row SQL parsing/planning - the fastest bulk-load
        path Postgres offers, the same mechanism `psql \\copy` uses.

        Parameters
        ----------
        truncate : if True, empties the table before loading (snapshot replace).
                   Set False for incremental appends.

        Returns the number of rows loaded.
        """
        if df.empty:
            self.log.warning("[%s] DataFrame is empty, nothing to load", table_name)
            return 0

        cols = list(df.columns)
        col_list = ",".join(f'"{c}"' for c in cols)  # quoted, safe against reserved words

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        with self.engine.begin() as conn:
            if truncate:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))

            raw = conn.connection.driver_connection
            with raw.cursor() as cur:
                cur.copy_expert(
                    f"COPY {table_name} ({col_list}) FROM STDIN WITH CSV NULL '\\N'",
                    buf,
                )

        self.log.info("[%s] loaded %d row(s) (truncate=%s)", table_name, len(df), truncate)
        return len(df)

    # -- incremental control table ------------------------------------------ #

    def get_processed_keys(self, table_name: str) -> set[str]:
        """Return the set of file_keys already recorded as loaded for `table_name`."""
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT file_key FROM bronze._processed_files WHERE table_name = :t"),
                {"t": table_name},
            ).scalars().all()
        return set(rows)

    def load_and_mark(self, df: pd.DataFrame, table_name: str, file_key: str) -> int:
        """Append one incremental file's rows and record it as processed - in a
        single transaction, so a failed load can never be mistakenly marked done.
        """
        if df.empty:
            self.log.warning("[%s] file '%s' produced 0 rows, skipping", table_name, file_key)
            return 0

        cols = list(df.columns)
        col_list = ",".join(f'"{c}"' for c in cols)

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="\\N")
        buf.seek(0)

        with self.engine.begin() as conn:
            raw = conn.connection.driver_connection
            with raw.cursor() as cur:
                cur.copy_expert(
                    f"COPY {table_name} ({col_list}) FROM STDIN WITH CSV NULL '\\N'",
                    buf,
                )
            conn.execute(
                text(
                    """
                    INSERT INTO bronze._processed_files (table_name, file_key, row_count)
                    VALUES (:t, :k, :n)
                    ON CONFLICT (table_name, file_key) DO NOTHING
                    """
                ),
                {"t": table_name, "k": file_key, "n": len(df)},
            )

        self.log.info("[%s] loaded %d row(s) from '%s'", table_name, len(df), file_key)
        return len(df)


# --------------------------------------------------------------------------- #
# Pipeline orchestration
# --------------------------------------------------------------------------- #

class BronzeIngestionPipeline:
    """Ties the reader and loader together and drives the load for each source.

    `SOURCES[table_name]["pattern"]` decides which private `_load_*` method
    runs - the dispatch logic itself never needs to change when a new source
    is added, only the SOURCES config does.
    """

    def __init__(self, reader: DataLakeReader, loader: PostgresWarehouseLoader, sources: dict):
        self.reader = reader
        self.loader = loader
        self.sources = sources
        self.log = logging.getLogger(f"{LOGGER_ROOT_NAME}.orchestrator")

    def load_table(self, table_name: str) -> None:
        """Load a single table using whichever strategy its SOURCES entry specifies."""
        if table_name not in self.sources:
            raise KeyError(f"'{table_name}' is not defined in SOURCES")

        config = self.sources[table_name]
        pattern = config["pattern"]
        prefix = config["prefix"]

        self.log.info("Loading '%s' (pattern=%s, prefix=%s)", table_name, pattern, prefix)

        if pattern == "snapshot_tagged":
            self._load_snapshot(table_name, self.reader.get_latest_tagged_file(prefix))

        elif pattern == "snapshot_partition":
            self._load_snapshot(table_name, self.reader.get_latest_snapshot_partition_files(prefix))

        elif pattern == "incremental":
            self._load_incremental(table_name, prefix)

        else:
            raise ValueError(f"Unknown pattern '{pattern}' for table '{table_name}'")

    def _load_snapshot(self, table_name: str, file_info: tuple[str, str]) -> None:
        """Pattern A: truncate the table, reload it from a single full-state file."""
        key, modified_at = file_info
        self.log.info("[%s] using latest file: %s (modified %s)", table_name, key, modified_at)
        df = self.reader.read_keys_to_df([key])
        self.loader.load_df(df, table_name, truncate=True)

    def _load_incremental(self, table_name: str, prefix: str) -> None:
        """Pattern B: load only files not already recorded in the control table."""
        files = self.reader.list_files_with_meta(prefix)
        processed_keys = self.loader.get_processed_keys(table_name)
        new_keys = [key for key, _ in files if key not in processed_keys]

        if not new_keys:
            self.log.info(
                "[%s] no new files to load (%d already processed)", table_name, len(processed_keys)
            )
            return

        self.log.info("[%s] %d new file(s) to load", table_name, len(new_keys))
        for key in new_keys:
            df = self.reader.read_keys_to_df([key])
            self.loader.load_and_mark(df, table_name, key)

    def run(self, table_names: Optional[list[str]] = None) -> None:
        """Run the pipeline for every source, or just the given table names.

        One table's failure is logged and skipped rather than aborting the
        whole run, so a single bad source doesn't block every other load.
        """
        targets = table_names or list(self.sources.keys())
        self.log.info("Starting bronze ingestion for %d table(s)", len(targets))

        succeeded: list[str] = []
        failed: list[str] = []

        for table_name in targets:
            try:
                self.load_table(table_name)
                succeeded.append(table_name)
            except Exception:
                self.log.exception("Failed to load '%s'", table_name)
                failed.append(table_name)

        self.log.info("Pipeline finished. Succeeded: %d, Failed: %d", len(succeeded), len(failed))
        if failed:
            self.log.warning("Failed tables: %s", ", ".join(failed))


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def build_pipeline() -> BronzeIngestionPipeline:
    """Wire up the S3 client, Postgres engine, and return a ready-to-run pipeline."""
    log = logging.getLogger(f"{LOGGER_ROOT_NAME}.bootstrap")

    log.info("Connecting to MinIO at %s", CONFIG["storage"]["minio_endpoint"])
    s3_client = boto3.client(
        "s3",
        endpoint_url=CONFIG["storage"]["minio_endpoint"],
        aws_access_key_id=CONFIG["storage"]["minio_access_key"],
        aws_secret_access_key=CONFIG["storage"]["minio_secret_key"],
    )
    bucket = CONFIG["storage"]["bronze_bucket"]

    log.info("Connecting to Postgres analytics database")
    engine = create_engine(CONFIG["database"]["analytics"]["analytics_url"])

    reader = DataLakeReader(s3_client=s3_client, bucket=bucket)
    loader = PostgresWarehouseLoader(engine=engine)
    loader.create_bronze_schema()

    return BronzeIngestionPipeline(reader=reader, loader=loader, sources=SOURCES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Parquet files from the data lake into the Postgres bronze schema."
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Specific bronze.* table names to load (default: all tables in SOURCES)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional path to also write logs to a file, in addition to the console",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logger(level=args.log_level, log_file=args.log_file)

    pipeline = build_pipeline()
    pipeline.run(table_names=args.tables)


if __name__ == "__main__":
    main()
