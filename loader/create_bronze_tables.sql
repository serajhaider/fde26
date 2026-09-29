
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.api_geolocation (
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat             DOUBLE PRECISION,
    geolocation_lng             DOUBLE PRECISION,
    geolocation_city            VARCHAR(100),
    geolocation_state           VARCHAR(2),
    _source_file                TEXT,
    _ingested_at                TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.api_translations (
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100),
    _source_file                   TEXT,
    _ingested_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.stream_order_payments (
    order_id             VARCHAR(64),
    payment_sequential   INTEGER,
    payment_type         VARCHAR(30),
    payment_installments INTEGER,
    payment_value        NUMERIC(12,2),
    _source_file         TEXT,
    _ingested_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.stream_order_reviews (
    review_id               VARCHAR(64),
    order_id                VARCHAR(64),
    review_score            SMALLINT,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _source_file            TEXT,
    _ingested_at             TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_orders (
    order_id                       VARCHAR(64),
    customer_id                    VARCHAR(64),
    order_status                   VARCHAR(20),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    _source_file                   TEXT,
    _ingested_at                   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_order_items (
    order_id             VARCHAR(64),
    order_item_id        INTEGER,
    product_id           VARCHAR(64),
    seller_id            VARCHAR(64),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(12,2),
    freight_value        NUMERIC(12,2),
    _source_file         TEXT,
    _ingested_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_customers (
    customer_id               VARCHAR(64),
    customer_unique_id        VARCHAR(64),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            VARCHAR(2),
    _source_file              TEXT,
    _ingested_at              TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_products (
    product_id                   VARCHAR(64),
    product_category_name        VARCHAR(100),
    product_name_lenght          NUMERIC,
    product_description_lenght   NUMERIC,
    product_photos_qty           NUMERIC,
    product_weight_g             NUMERIC(10,2),
    product_length_cm            NUMERIC(10,2),
    product_height_cm            NUMERIC(10,2),
    product_width_cm             NUMERIC(10,2),
    _source_file                 TEXT,
    _ingested_at                 TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.pg_sellers (
    seller_id               VARCHAR(64),
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(2),
    _source_file            TEXT,
    _ingested_at            TIMESTAMP DEFAULT now()
);
