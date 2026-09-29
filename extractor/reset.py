"""
Utility script to completely empty all objects from the S3 Bronze Bucket.

"""

import sys
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

try:
    from config_loader import CONFIG
    DEFAULT_BUCKET = CONFIG["storage"]["bronze_bucket"]
except ImportError:
    DEFAULT_BUCKET = "raw-bronze"

def get_minio_resource():
    """Returns a boto3 S3 resource connected to local MinIO."""
    return boto3.resource(
        "s3",
        endpoint_url=CONFIG["storage"]["minio_endpoint"],
        aws_access_key_id=CONFIG["storage"]["minio_access_key"],
        aws_secret_access_key=CONFIG["storage"]["minio_secret_key"],
        region_name="us-east-1",  # Dummy region required by boto3
        )

def empty_s3_bucket(bucket_name: str = "raw-bronze", force: bool = False):
    s3_resource = get_minio_resource()
    bucket = s3_resource.Bucket(bucket_name)

    print("=" * 65)
    print(f"⚠️  WARNING: You are about to DELETE ALL OBJECTS in bucket:")
    print(f"👉 s3://{bucket_name}")
    print("=" * 65)

    # 1. Safety check
    if not force:
        confirm = input(
            f"Are you sure you want to permanently empty '{bucket_name}'? (type 'yes' to confirm): "
        )
        if confirm.strip().lower() != "yes":
            print("❌ Operation cancelled by user. No objects were deleted.")
            sys.exit(0)

    print("\n🧹 Starting cleanup process...")

    try:

        # Delete all object versions and delete markers (handles versioned & unversioned buckets)
        bucket.object_versions.delete()

        print(f"✅ Successfully emptied bucket: s3://{bucket_name}")

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            print(f"❌ Error: Bucket '{bucket_name}' does not exist.")
        else:
            print(f"❌ AWS Client Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error while emptying bucket: {e}")


if __name__ == "__main__":
    empty_s3_bucket(bucket_name=DEFAULT_BUCKET, force= False)