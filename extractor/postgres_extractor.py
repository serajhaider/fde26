import io
import json
import logging
from datetime import datetime
import pandas as pd
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, inspect, text

from extractor.base_extractor import BaseExtractor
from config_loader import CONFIG

logger = logging.getLogger(__name__)


class PostgresSnapshotExtractor(BaseExtractor):
    """Extracts state snapshots based on Postgres xmin tracking."""

    def __init__(
        self,
        tables_to_snapshot: list[str] = None,
        schema: str = None,
        bucket_name: str = None,
    ):
        super().__init__(bucket_name=bucket_name)
        self.tables_to_snapshot = ["customers", "sellers", "products"] if tables_to_snapshot is None else tables_to_snapshot
        self.schema = "public" if schema is None else schema
        self.engine = create_engine(CONFIG["database"]["oltp"]["oltp_url"])

    def _get_last_synced_xmin(self, table: str) -> int:
        """Fetch stored xmin state file from S3."""
        state_key = f"metadata/table_name={table}/state.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=state_key)
            state_data = json.loads(response["Body"].read().decode("utf-8"))
            return state_data.get("last_xmin", 0)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(f"No previous xmin state found for '{table}'. Defaulting to 0.")
                return 0  # First run
            raise e

    def _update_synced_xmin(self, table: str, xmin: int):
        """Update stored xmin state in S3."""
        state_key = f"metadata/table_name={table}/state.json"
        payload = json.dumps({
            "table": table,
            "last_xmin": xmin,
            "updated_at": datetime.now().isoformat(),
        })

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=state_key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )

    def run(self):
        """Executes the Postgres xmin Snapshot Pipeline."""
        with self.engine.connect() as conn:
            inspector = inspect(self.engine)
            existing_tables = set(inspector.get_table_names(schema=self.schema))
            valid_tables = set(self.tables_to_snapshot) & existing_tables

            missing_tables = set(self.tables_to_snapshot) - existing_tables
            if missing_tables:
                logger.warning(f"These configured tables don't exist and will be skipped: {missing_tables}")

            for table in valid_tables:
                logger.info(f"--- [Snapshot] Checking table: {table} ---")

                # Check the current max xmin
                xmin_query = text(f"SELECT COALESCE(MAX(xmin::text::bigint), 0) FROM {self.schema}.{table}")
                current_max_xmin = conn.execute(xmin_query).scalar()

                # Fetch the last synced xmin
                last_xmin = self._get_last_synced_xmin(table)

                # Skip if nothing changed
                if current_max_xmin <= last_xmin and last_xmin != 0:
                    logger.info(f"⏭️  No changes in '{table}'. Skipping snapshot.")
                    continue

                logger.info(f"🔄 Extracting '{table}' (xmin: {last_xmin} ➔ {current_max_xmin})...")

                # Extract full table
                df = pd.read_sql(
                    text(f"SELECT * FROM {self.schema}.{table};"),
                    con=self.engine,
                )

                # Convert to Parquet in-memory
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False, engine="pyarrow")
                buffer.seek(0)

                # Build Hive-partitioned key
                now = datetime.now()
                snapshot_date = now.strftime("%Y-%m-%d")
                snapshot_time = now.strftime("%H%M%S")
                s3_key = f"postgres/snapshots/table_name={table}/snapshot_date={snapshot_date}/snapshot_{snapshot_time}.parquet"

                # Upload and update state
                self.s3_client.upload_fileobj(buffer, self.bucket_name, s3_key)
                self._update_synced_xmin(table=table, xmin=current_max_xmin)

                logger.info(f"✅ Uploaded to s3://{self.bucket_name}/{s3_key}")


class PostgresIncrementalExtractor(BaseExtractor):
    """Extracts incremental delta streams based on timestamp watermarks."""

    def __init__(
        self,
        high_velocity_tables: dict[str, dict] = None,
        schema: str = None,
        bucket_name: str = None,
    ):
        super().__init__(bucket_name=bucket_name)

        self.high_velocity_tables = {
            "orders": {
                "ts_col": "order_purchase_timestamp",
                "join_sql": "",
            },
            "order_items": {
                "ts_col": "o.order_purchase_timestamp",
                "join_sql": "JOIN public.orders o ON order_items.order_id = o.order_id",
            },
        } if high_velocity_tables is None else high_velocity_tables

        self.schema = "public" if schema is None else schema
        self.engine = create_engine(CONFIG["database"]["oltp"]["oltp_url"])

    def _get_last_watermark(self, table: str) -> str:
        """Fetch stored timestamp watermark from S3 state file."""
        state_key = f"metadata/table_name={table}/watermark.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=state_key)
            state_data = json.loads(response["Body"].read().decode("utf-8"))
            return state_data.get("last_watermark", "1970-01-01 00:00:00")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(f"No previous watermark found for '{table}'. Defaulting to epoch.")
                return "1970-01-01 00:00:00"
            raise e

    def _update_watermark(self, table: str, max_timestamp: str):
        """Save the latest timestamp watermark to S3 state file."""
        state_key = f"metadata/table_name={table}/watermark.json"
        payload = json.dumps({
            "table": table,
            "last_watermark": str(max_timestamp),
            "updated_at": datetime.now().isoformat(),
        })

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=state_key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )

    def run(self):
        """Executes the Incremental Watermark Pipeline."""
        with self.engine.connect() as conn:
            for table, config in self.high_velocity_tables.items():
                ts_col = config["ts_col"]
                join_sql = config["join_sql"]

                logger.info(f"--- [Incremental] Processing Table: {table} ---")

                # Fetch the last watermark
                last_watermark = self._get_last_watermark(table=table)

                # Build and run the parameterized delta query
                query = text(f"""
                    SELECT {self.schema}.{table}.*, {ts_col} AS tracking_ts
                    FROM {self.schema}.{table}
                    {join_sql}
                    WHERE {ts_col} > :watermark
                    ORDER BY {ts_col} ASC
                """)

                df = pd.read_sql(query, conn, params={"watermark": last_watermark})

                # Skip if no new rows
                if df.empty:
                    logger.info(f"⏭️  No new rows found in '{table}'. Skipping batch.")
                    continue

                # Derive new watermark, drop helper column
                max_new_watermark = df["tracking_ts"].max()
                df = df.drop(columns=["tracking_ts"])

                logger.info(f"🔥 Extracted {len(df)} new records (New Max Watermark: {max_new_watermark})")

                # Convert to Parquet in-memory
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False, engine="pyarrow")
                buffer.seek(0)

                # Build Hive-partitioned key
                now = datetime.now()
                s3_key = (
                    f"postgres/batches/table_name={table}/"
                    f"batch_date={now.strftime('%Y-%m-%d')}/batch_{now.strftime('%H%M%S')}.parquet"
                )

                # Upload and update watermark
                self.s3_client.upload_fileobj(buffer, self.bucket_name, s3_key)
                self._update_watermark(table, max_new_watermark)

                logger.info(f"✅ Batch uploaded to s3://{self.bucket_name}/{s3_key}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    snapshot_runner = PostgresSnapshotExtractor(bucket_name="test-1")
    snapshot_runner.run()

    incremental_runner = PostgresIncrementalExtractor(bucket_name="test-1")
    incremental_runner.run()