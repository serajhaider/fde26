import os
import time
import logging
import pandas as pd
from datetime import timedelta

from dotenv import load_dotenv
load_dotenv(override=True)

from config_loader import CONFIG

from producer.simulate.db_ops import init_schema, seed_master_tables, get_latest_order_time, insert_orders_transaction
from producer.simulate.webhooks import send_webhook

# ---------------------------------------------------------
# Configuration Setup
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

SIM_SPEED = int(CONFIG["simulation"]["sim_speed"])
DATA_DIR = CONFIG["simulation"]["data_dir"]
PRELOAD_WATERMARK = pd.to_datetime(CONFIG["simulation"]["preload_watermark"])

def load_streaming_datasets():
    logger.info("Loading streaming datasets into memory...")
    df_orders = pd.read_csv(os.path.join(DATA_DIR, "olist_orders_dataset.csv"))
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    df_orders = df_orders.sort_values("order_purchase_timestamp")
    
    df_items = pd.read_csv(os.path.join(DATA_DIR, "olist_order_items_dataset.csv"))
    df_payments = pd.read_csv(os.path.join(DATA_DIR, "olist_order_payments_dataset.csv"))
    
    df_reviews = pd.read_csv(os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv"))
    #BEFORE
    # df_reviews['review_creation_date'] = pd.to_datetime(df_reviews['review_creation_date'])
    #AFTER
    df_reviews['review_answer_timestamp'] = pd.to_datetime(df_reviews['review_answer_timestamp'])
    df_reviews = df_reviews.sort_values("review_answer_timestamp")

    
    return df_orders, df_items, df_payments, df_reviews

def synchronize_start(df_orders, df_items, df_payments, df_reviews):
    """Figures out where to start time, and handles historical preload if DB is empty."""
    last_db_time = get_latest_order_time()
        
    if last_db_time:
        sim_start = pd.to_datetime(last_db_time)
        logger.info(f"Resuming simulation from watermark: {sim_start}")
        return sim_start

    logger.info(f"Database empty. Preloading historical data up to {PRELOAD_WATERMARK}...")
    
    preload_orders = df_orders[df_orders['order_purchase_timestamp'] <= PRELOAD_WATERMARK]

    #BEFORE
    # preload_reviews = df_reviews[df_reviews['review_creation_date'] <= PRELOAD_WATERMARK]
    # AFTER
    preload_reviews = df_reviews[df_reviews['review_answer_timestamp'] <= PRELOAD_WATERMARK]

    if not preload_orders.empty:
        preload_order_ids = preload_orders['order_id'].tolist()
        preload_items = df_items[df_items['order_id'].isin(preload_order_ids)]
        preload_payments = df_payments[df_payments['order_id'].isin(preload_order_ids)]
        
        # 1. Sync DB first
        if insert_orders_transaction(preload_orders, preload_items):
            # 2. Sync Webhooks only if DB succeeds
            send_webhook("order_payments", preload_payments)
            send_webhook("order_reviews", preload_reviews)
            
    return PRELOAD_WATERMARK

def start_simulation():
    df_orders, df_items, df_payments, df_reviews = load_streaming_datasets()
    sim_start_time = synchronize_start(df_orders, df_items, df_payments, df_reviews)
    
    logger.info("🚀 Starting simulation engine ticker loop...")
    real_start_time = time.time()
    last_sim_time = sim_start_time
    
    while True:
        elapsed_real_seconds = time.time() - real_start_time
        current_sim_time = sim_start_time + timedelta(seconds=elapsed_real_seconds * SIM_SPEED)
        
        # 1. Slice time windows
        new_orders = df_orders[
            (df_orders['order_purchase_timestamp'] > last_sim_time) & 
            (df_orders['order_purchase_timestamp'] <= current_sim_time)
        ]

        # BEFORE
        # new_reviews = df_reviews[
        #     (df_reviews['review_creation_date'] > last_sim_time) & 
        #     (df_reviews['review_creation_date'] <= current_sim_time)
        # ]

        # AFTER
        new_reviews = df_reviews[
            (df_reviews['review_answer_timestamp'] > last_sim_time) & 
            (df_reviews['review_answer_timestamp'] <= current_sim_time)
        ]
        
        # 2. Process Orders/Items/Payments
        if not new_orders.empty:
            logger.info(f"[Tick: {current_sim_time.strftime('%Y-%m-%d %H:%M')}] Processing {len(new_orders)} new orders.")
            
            new_order_ids = new_orders['order_id'].tolist()
            new_items = df_items[df_items['order_id'].isin(new_order_ids)]
            new_payments = df_payments[df_payments['order_id'].isin(new_order_ids)]
            
            # Sync: Only send payment webhook if Postgres transaction succeeds
            if insert_orders_transaction(new_orders, new_items):
                send_webhook("order_payments", new_payments)

        # 3. Process Reviews (Runs in parallel, independent of orders)
        if not new_reviews.empty:
            send_webhook("order_reviews", new_reviews)
        
        last_sim_time = current_sim_time
        time.sleep(1)

if __name__ == "__main__":
    logger.info("Waiting 5 seconds for PostgreSQL to accept connections...")
    time.sleep(5) 
    
    init_schema()
    seed_master_tables(DATA_DIR)
    
    try:
        start_simulation()
    except KeyboardInterrupt:
        logger.info("🛑 Simulation stopped safely by user (Ctrl+C).")