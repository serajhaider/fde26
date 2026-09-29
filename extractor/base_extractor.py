from abc import ABC, abstractmethod
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from config_loader import CONFIG

import logging


# Configure a module-level logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract Base Class for all source extractors."""

    def __init__(self, bucket_name: str = None):

        # Get bucket name from argument or configuration
        self.bucket_name = (
            bucket_name
            if bucket_name
            else CONFIG["storage"]["bronze_bucket"]
        )

        # Create MinIO/S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=CONFIG["storage"]["minio_endpoint"],
            aws_access_key_id=CONFIG["storage"]["minio_access_key"],
            aws_secret_access_key=CONFIG["storage"]["minio_secret_key"],
            region_name="us-east-1",
            config=Config(signature_version="s3v4"),
        )

        # Check/create bucket
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Ensure target raw-bronze bucket exists. Create it otherwise."""

        try:
            # Check whether bucket exists
            self.s3_client.head_bucket(
                Bucket=self.bucket_name
            )

            logger.info(
                f"Bucket '{self.bucket_name}' already exists."
            )

        except ClientError as e:

            error_code = e.response.get("Error", {}).get("Code", "")

            if error_code in ["404", "NoSuchBucket", "NotFound"]:

                # Bucket does not exist → create it
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name
                )

                logger.info(
                    f"Bucket '{self.bucket_name}' created successfully."
                )

            else:
                logger.error(
                    f"Error checking/creating bucket "
                    f"'{self.bucket_name}': {e}"
                )
                raise

    @abstractmethod
    def run(self):
        """Execution interface to be implemented by child extractors."""
        pass