import os
import logging
import requests
import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv(override=True)

from config_loader import CONFIG
# Set up logging for this module
logger = logging.getLogger(__name__)

FASTAPI_URL = 'http://localhost:8000'  # Ensure this matches your FastAPI server URL

# Optimization: Use a Session to reuse TCP connections across thousands of requests
http_session = requests.Session()

def send_webhook(source_table: str, df_subset: pd.DataFrame) -> None:
    """
    Serializes a DataFrame subset and streams it to the FastAPI webhook.
    Uses connection pooling and robust timeout handling for long-running stability.
    """
    if df_subset.empty:
        return

    # Create a local copy to prevent mutating the original cached DataFrame
    df_json = df_subset.copy()
    
    # Safely stringify datetime columns for JSON serialization
    datetime_cols = df_json.select_dtypes(include=['datetime64[ns]', 'datetime64', 'datetimetz']).columns
    for col in datetime_cols:
        df_json[col] = df_json[col].astype(str)

    # 2. BULLETPROOF FIX: Cast everything to object so Pandas allows 'None' 
    # instead of forcing floats back to 'NaN'
    df_json = df_json.astype(object).replace({np.nan: None})

    payload = {
        "source_table": source_table,
        "records": df_json.to_dict(orient="records")
    }

    try:
        response = http_session.post(
            f"{FASTAPI_URL}/api/v1/webhook/{source_table}",
            json=payload,
            timeout=10 # Prevents hanging if FastAPI is temporarily overwhelmed
        )
        response.raise_for_status()
        logger.info(f"[Webhook] 🌐 Successfully sent {len(df_json)} rows to '{source_table}'")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[Webhook Error] ❌ Failed to dispatch '{source_table}': {e}")