import io
import time
import logging
from datetime import datetime
import pandas as pd
import requests
from botocore.exceptions import ClientError

from extractor.base_extractor import BaseExtractor
from config_loader import CONFIG

logger = logging.getLogger(__name__)


class BaseAPIExtractor(BaseExtractor):
    """Base API extractor with S3 key existence checks and archiving support."""

    def object_exists_in_s3(self, s3_key: str) -> bool:
        """Check if an object exists in the S3 bronze bucket using a lightweight HEAD call."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            # 404 means key does not exist; any other error should be raised
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Unexpected error checking existence of '{s3_key}': {e}")
            raise

    def _archive_existing_object(self, key: str):
        """
        If an object exists at `key`, copy it to a new '<name>_old_<timestamp>.<ext>'
        key in the same directory, then delete the original — simulating a rename,
        since S3-compatible stores have no native rename operation.
        """
        if not self.object_exists_in_s3(key):
            logger.info(f"No existing object at '{key}' to archive.")
            return

        directory, filename = key.rsplit("/", 1)
        name, ext = filename.rsplit(".", 1)
        name = name.replace("_latest", "")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_key = f"{directory}/{name}_old_{timestamp}.{ext}"

        self.s3_client.copy_object(
            Bucket=self.bucket_name,
            CopySource={"Bucket": self.bucket_name, "Key": key},
            Key=archived_key,
        )
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)

        logger.info(f"📦 Archived existing '{key}' ➔ '{archived_key}'")


class TranslationAPIExtractor(BaseAPIExtractor):
    """Full-load extractor for small translation endpoint."""

    def __init__(self, api_url: str = None, bucket_name: str = None):
        super().__init__(bucket_name=bucket_name)
        self.api_url = api_url or f"{CONFIG['api']['fastapi_url']}/translation"
        self.latest_key = "api/api_endpoint=translations/translations_latest.parquet"

    def run(self, force_refresh: bool = False):
        logger.info("--- [API Extractor] Fetching Translation Data ---")

        # 1. Skip check — bypassed entirely if force_refresh is True
        if not force_refresh and self.object_exists_in_s3(self.latest_key):
            logger.info(f"⏭️  '{self.latest_key}' already exists. Skipping. (pass force_refresh=True to override)")
            return

        # 2. If forcing a refresh, archive whatever is currently at the latest key
        if force_refresh:
            self._archive_existing_object(self.latest_key)

        # 3. Fetch data from API
        response = requests.get(self.api_url, timeout=10)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data)

        if df.empty:
            logger.info("⏭️  Translation data is empty. Skipping upload.")
            return

        # 4. Convert to Parquet in-memory and upload directly to the latest key
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.s3_client.upload_fileobj(buffer, self.bucket_name, self.latest_key)

        logger.info(f"✅ Extracted {len(df)} translation records ➔ s3://{self.bucket_name}/{self.latest_key}")


class GeolocationAPIExtractor(BaseAPIExtractor):
    """Paginated cursor-following extractor for geolocation endpoint."""

    def __init__(self, start_url: str = None, limit: int = 100000, bucket_name: str = None):
        super().__init__(bucket_name=bucket_name)
        self.start_url = start_url or f"{CONFIG['api']['fastapi_url']}/geolocation?page=1&limit={limit}"
        self.latest_key = "api/api_endpoint=geolocation/geolocation_latest.parquet"

    def run(self, force_refresh: bool = False):
        logger.info("--- [API Extractor] Fetching Geolocation Data ---")

        # 1. Skip check — bypassed entirely if force_refresh is True
        if not force_refresh and self.object_exists_in_s3(self.latest_key):
            logger.info(f"⏭️  '{self.latest_key}' already exists. Skipping. (pass force_refresh=True to override)")
            return

        # 2. If forcing a refresh, archive whatever is currently at the latest key
        if force_refresh:
            self._archive_existing_object(self.latest_key)

        # 3. Fetch data via pagination links
        current_url = self.start_url
        all_records = []
        session = requests.Session()

        while current_url:
            response = session.get(current_url, timeout=15)
            response.raise_for_status()

            payload = response.json()
            page_data = payload.get("data", [])
            all_records.extend(page_data)

            current_url = payload.get("next_url")
            time.sleep(0.05)  # Throttling protection

        df = pd.DataFrame(all_records)

        if df.empty:
            logger.info("⏭️  No geolocation records retrieved. Skipping upload.")
            return

        # 4. Convert to Parquet in-memory and upload directly to the latest key
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.s3_client.upload_fileobj(buffer, self.bucket_name, self.latest_key)

        logger.info(f"✅ Uploaded total {len(df)} geolocation records ➔ s3://{self.bucket_name}/{self.latest_key}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Run Translation Extractor
    trans_extractor = TranslationAPIExtractor()
    # trans_extractor.run()

    # Run Geolocation Extractor
    geo_extractor = GeolocationAPIExtractor()
    # geo_extractor.run()

    # Forced re-extraction example — archives the old "latest" file, writes a fresh one
    trans_extractor.run(force_refresh=True)
    geo_extractor.run(force_refresh=True)