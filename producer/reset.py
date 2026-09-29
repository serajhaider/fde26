import boto3
import psycopg2
import sys
from config_loader import CONFIG

def get_minio_resource():
    """Returns a boto3 S3 resource connected to local MinIO."""
    return boto3.resource(
        "s3",
        endpoint_url=CONFIG["storage"]["minio_endpoint"],
        aws_access_key_id=CONFIG["storage"]["minio_access_key"],
        aws_secret_access_key=CONFIG["storage"]["minio_secret_key"],
        region_name="us-east-1",  # Dummy region required by boto3
        )

def empty_s3_bucket(bucket_name):
    print(f"\n[S3] Connecting to bucket: {bucket_name}...")
    try:
        s3 = get_minio_resource()
        bucket = s3.Bucket(bucket_name)
        
        # .object_versions.delete() handles both versioned and unversioned buckets
        # It deletes all objects, versions, and delete markers.
        response = bucket.object_versions.delete()
        
        if response:
            print(f"[S3] Successfully emptied bucket '{bucket_name}'.")
        else:
            print(f"[S3] Bucket '{bucket_name}' was already empty.")
            
    except Exception as e:
        print(f"[S3] ERROR: Failed to empty bucket. Details: {e}")
        sys.exit(1)

def reset_postgres_db(db_config):
    print(f"\n[DB] Connecting to PostgreSQL database '{db_config['dbname']}' at {db_config['host']}...")
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_config)
        
        # Set autocommit to True so we don't have to manually commit schema changes
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("[DB] Dropping public schema (Cascade)...")
        cursor.execute("DROP SCHEMA public CASCADE;")
        
        print("[DB] Recreating empty public schema...")
        cursor.execute("CREATE SCHEMA public;")
        
        print("[DB] Restoring schema permissions...")
        cursor.execute("GRANT ALL ON SCHEMA public TO public;")
        
        cursor.close()
        conn.close()
        print("[DB] Successfully wiped all tables from the database.")
        
    except Exception as e:
        print(f"[DB] ERROR: Failed to reset database. Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # --- CONFIGURATION ---
    TARGET_BUCKET = CONFIG["storage"]["shared_bucket"]  # Change this to the bucket you want to empty
    
    DB_CONFIG = {
        "host": CONFIG["database"]["oltp"]["host"],
        "port": CONFIG["database"]["oltp"]["port"],
        "dbname": CONFIG["database"]["oltp"]["name"],
        "user": CONFIG["database"]["oltp"]["user"],
        "password": CONFIG["database"]["oltp"]["password"],
    }
    
    # --- SAFETY CHECK ---
    print("!!! DANGER ZONE !!!")
    print(f"This will DESTROY ALL DATA in S3 bucket: '{TARGET_BUCKET}'")
    print(f"This will DESTROY ALL TABLES in Postgres DB: '{DB_CONFIG['dbname']}'")
    
    confirmation = input("\nAre you absolutely sure? Type 'DELETE EVERYTHING' to proceed: ")
    
    if confirmation == "DELETE EVERYTHING":
        empty_s3_bucket(TARGET_BUCKET)
        reset_postgres_db(DB_CONFIG)
        print("\nAll destructive operations completed successfully.")
    else:
        print("\nOperation aborted. No data was deleted.")