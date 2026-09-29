import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError

from extractor.base_extractor import BaseExtractor
from config_loader import CONFIG

logger = logging.getLogger(__name__)


class ExternalS3Extractor(BaseExtractor):
    """Copies incoming third-party files directly from pay-review bucket to raw-bronze."""

    def __init__(
        self,
        source_bucket: str = None,
        source_tables: list[str] = None,
        bucket_name: str = None,
    ):
        super().__init__(bucket_name=bucket_name)
        self.source_bucket = "pay-review" if source_bucket is None else source_bucket
        self.source_tables = ["order_payments", "order_reviews"] if source_tables is None else source_tables

    def _get_last_synced_timestamp(self, table: str) -> str:
        """Retrieves last synced timestamp state from raw-bronze."""
        state_key = f"metadata/external_table={table}/state.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=state_key)
            state_data = json.loads(response["Body"].read().decode("utf-8"))
            return state_data.get("last_synced_timestamp", "1970-01-01T00:00:00+00:00")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(f"No previous state found for '{table}'. Defaulting to epoch.")
                return "1970-01-01T00:00:00+00:00"  # Initial state
            logger.error(f"Unexpected error fetching state for '{table}': {e}")
            raise e

    def _update_synced_timestamp(self, table: str, max_timestamp: str):
        """Updates last synced timestamp in raw-bronze metadata."""
        state_key = f"metadata/external_table={table}/state.json"
        payload = json.dumps({
            "table": table,
            "last_synced_timestamp": max_timestamp,
            "updated_at": datetime.now().isoformat(),
        })

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=state_key,
            Body=payload.encode("utf-8"),
            ContentType="application/json",
        )

    def run(self):
        """Finds newly arrived files in pay-review and copies them server-side to raw-bronze."""
        for table in self.source_tables:
            logger.info(f"--- Checking for new '{table}' files in '{self.source_bucket}' ---")

            last_synced_ts = self._get_last_synced_timestamp(table)
            prefix = f"{table}/"

            # 1. Fetch objects from source bucket
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.source_bucket, Prefix=prefix)

            new_files = []
            max_ts_in_batch = last_synced_ts

            for page in pages:
                for obj in page.get("Contents", []):
                    file_modified_ts = obj["LastModified"].isoformat()

                    # Find files arrived after our last state run
                    if file_modified_ts > last_synced_ts:
                        new_files.append((obj["Key"], file_modified_ts))
                        if file_modified_ts > max_ts_in_batch:
                            max_ts_in_batch = file_modified_ts

            if not new_files:
                logger.info(f"⏭️  No new files in '{table}'. Skipping copy.")
                continue

            logger.info(f"🔥 Found {len(new_files)} new file(s) for '{table}'. Copying...")

            # 2. Server-side copy directly inside S3/MinIO
            for source_key, file_ts in new_files:
                filename = source_key.split("/")[-1]
                target_key = f"external_s3/stream_event={table}/{filename}"

                self.s3_client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={"Bucket": self.source_bucket, "Key": source_key},
                    Key=target_key,
                )

            # 3. Save new state
            self._update_synced_timestamp(table, max_ts_in_batch)
            logger.info(f"✅ Updated watermark state for '{table}' to {max_ts_in_batch}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    extractor = ExternalS3Extractor(
        source_bucket="pay-review",
        source_tables=["order_payments", "order_reviews"],
        bucket_name=CONFIG["storage"]["bronze_bucket"],
    )
    extractor.run()