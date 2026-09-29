import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
load_dotenv(override=True)

logger = logging.getLogger(__name__)

from config_loader import CONFIG

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INIT_SQL_PATH = os.path.join(CURRENT_DIR, "init.sql")

# Database connection parameters pulled directly from unified config
DB_USER = CONFIG["database"]["oltp"]["user"]
DB_PASS = CONFIG["database"]["oltp"]["password"]
DB_HOST = CONFIG["database"]["oltp"]["host"]
DB_PORT = CONFIG["database"]["oltp"]["port"]
DB_NAME = CONFIG["database"]["oltp"]["name"]
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(DB_URL)

engine = create_engine(DB_URL, pool_pre_ping=True)

def init_schema():
    logger.info("Initializing database schema...")
    try:
        with open(INIT_SQL_PATH, "r") as file:
            sql_script = file.read()
        with engine.begin() as conn:
            conn.execute(text(sql_script))
        logger.info("✅ Schema created/verified successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize schema: {e}")
        raise

def seed_master_tables(data_dir: str):
    logger.info("Checking if master tables need seeding...")
    try:
        with engine.connect() as conn:
            if conn.execute(text("SELECT COUNT(*) FROM customers")).scalar() > 0:
                logger.info("Database already populated. Skipping master seed.")
                return
    except SQLAlchemyError:
        pass 

    logger.info("Loading static master data...")
    tables = {
        "customers": "olist_customers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv"
    }

    with engine.begin() as conn:
        for table, filename in tables.items():
            df = pd.read_csv(os.path.join(data_dir, filename))
            df.to_sql(table, conn, if_exists="append", index=False)
            logger.info(f"   -> Seeded {len(df)} records into '{table}'")

def get_latest_order_time():
    """Returns the latest order timestamp in the DB, or None if empty."""
    with engine.connect() as conn:
        return conn.execute(text("SELECT MAX(order_purchase_timestamp) FROM orders")).scalar()

def insert_orders_transaction(df_orders: pd.DataFrame, df_items: pd.DataFrame) -> bool:
    """Inserts orders and items atomically. Returns True if successful."""
    if df_orders.empty:
        return True
        
    try:
        with engine.begin() as conn:
            df_orders.to_sql("orders", conn, if_exists="append", index=False)
            if not df_items.empty:
                df_items.to_sql("order_items", conn, if_exists="append", index=False)
        logger.info(f"  -> 💾 DB Insert: {len(df_orders)} orders, {len(df_items)} items.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"  -> ❌ Postgres Transaction failed: {e}")
        return False