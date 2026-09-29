import io
import os
import boto3
import pandas as pd
from botocore.client import Config
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any

from producer.app.s3_config import *

app = FastAPI(
    title="Olist Ingestion Webhook Receiver",
    version="1.0.0",
    description="Ingestion gateway landing raw operational streams into MinIO Layer."
)


s3_client = get_s3_client()
if not test_connection(s3_client):
    sys.exit(1)
ensure_bucket_exists(s3_client,SHARED_BUCKET)

# ---------------------------------------------------------
# Pydantic Models for Validation
# ---------------------------------------------------------
class WebhookPayload(BaseModel):
    source_table: str
    records: List[Dict[str, Any]]

# ---------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------
@app.get("/")
def health_check():
    return {
        "status": "healthy",
        "storage_backend": "MinIO",
        "target_bucket": SHARED_BUCKET
    }

@app.post("/api/v1/webhook/{source_table}")
def receive_streaming_webhook(source_table: str, payload: WebhookPayload):
    """
    Unified production webhook receiver. Catches batch payloads, serializes them 
    into optimized Parquet files in-memory, and lands them into the MinIO Bronze Lake.
    """
    try:
        if not payload.records:
            return {
                "status": "success", 
                "source_table": source_table, 
                "inserted_records": 0,
                "message": "Payload was empty. No file written."
            }

        # Convert dictionary records to a structured pandas DataFrame
        df = pd.DataFrame(payload.records)
        
        # Serialize DataFrame directly to memory buffer as Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        # Capture the timestamp once to ensure date and time match perfectly
        now = pd.Timestamp.now()
        date_str = now.strftime('%Y-%m-%d')
        time_str = now.strftime('%H%M%S_%f')

        # Structure path with Hive-style date partitioning and high-precision time
        file_name = f"{source_table}/batch_date={date_str}/batch_{time_str}.parquet"
        
        # Stream upload to MinIO
        s3_client.upload_fileobj(buffer, SHARED_BUCKET, file_name)
        
        print(f"📥 [STREAM - {source_table}] Successfully ingested {len(df)} rows -> s3://{SHARED_BUCKET}/{file_name}")
        
        return {
            "status": "success",
            "source_table": source_table,
            "inserted_records": len(df),
            "destination_path": f"s3://{SHARED_BUCKET}/{file_name}"
        }

    except Exception as e:
        error_msg = f"Webhook processing failed for '{source_table}': {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)