"""
MinIO S3 Client — connection + bucket bootstrap.
"""
import os
import sys
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

from dotenv import load_dotenv
load_dotenv(override=True)

from config_loader import CONFIG

# ---- Configuration ----
RAW_ENDPOINT = CONFIG["storage"]["minio_endpoint"]
MINIO_ACCESS_KEY = CONFIG["storage"]["minio_access_key"]
MINIO_SECRET_KEY = CONFIG["storage"]["minio_secret_key"]
MINIO_REGION = "us-east-1"                 # MinIO ignores the value but boto3 requires one
MINIO_SECURE = False                       # True if endpoint uses https
SHARED_BUCKET = CONFIG["storage"]["shared_bucket"]

# Ensure endpoint has http:// or https:// for boto3
if not RAW_ENDPOINT.startswith("http://") and not RAW_ENDPOINT.startswith("https://"):
    scheme = "https://" if MINIO_SECURE else "http://"
    MINIO_ENDPOINT = f"{scheme}{RAW_ENDPOINT}"
else:
    MINIO_ENDPOINT = RAW_ENDPOINT

def get_s3_client(
    endpoint_url: str = MINIO_ENDPOINT,
    access_key: str = MINIO_ACCESS_KEY,
    secret_key: str = MINIO_SECRET_KEY,
    region: str = MINIO_REGION,
    secure: bool = MINIO_SECURE,
):
    """Create and return a boto3 S3 client configured for MinIO."""
    short_timeout_config = Config(
        connect_timeout=10,
        read_timeout=10,
        retries={'max_attempts': 10} # Don't keep retrying if it fails
        )

    client_kwargs = {
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": region,
        "config": short_timeout_config
        }

    return boto3.client("s3", **client_kwargs)


def test_connection(client) -> bool:
    """Verify the client can reach MinIO by listing buckets."""
    try:
        response = client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        print(f"Connected to MinIO. Existing buckets: {bucket_names}")
        return True
    except EndpointConnectionError:
        print(f"Could not reach MinIO endpoint: {MINIO_ENDPOINT}")
        return False
    except ClientError as e:
        print(f"MinIO responded with an error: {e.response['Error']['Message']}")
        return False


def ensure_bucket_exists(client, bucket_name: str = SHARED_BUCKET) -> bool:
    """Check if a bucket exists; create it if it doesn't. Returns True on success."""
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return True
    except ClientError as e:
        status_code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status_code == 404:
            try:
                client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created.")
                return True
            except ClientError as create_err:
                print(f"Failed to create bucket '{bucket_name}': {create_err}")
                return False
        else:
            print(f"Error checking bucket '{bucket_name}': {e}")
            return False

# Shared client instance
s3_client = get_s3_client()

# Run bootstrap check on import when FastAPI initializes
if test_connection(s3_client):
    ensure_bucket_exists(s3_client, SHARED_BUCKET)
    
# if __name__ == "__main__":
#     s3 = get_s3_client()

#     if not test_connection(s3):
#         sys.exit(1)

#     ensure_bucket_exists(s3, SHARED_BUCKET)