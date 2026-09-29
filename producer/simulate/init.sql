-- MASTER TABLES

CREATE TABLE IF NOT EXISTS customers(
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id  VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(50),
    customer_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght FLOAT,
    product_description_lenght FLOAT,
    product_photos_qty FLOAT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);


--  STREAMING  TABLES (Rely on Foreign Keys)

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    CONSTRAINT fk_orders_customers
        FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10, 2) NOT NULL,
    freight_value NUMERIC(10, 2) NOT NULL,
    -- Composite Primary Key: an order can have multiple items, 
    -- but order_item_id is unique within that specific order.
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_order_items_orders 
        FOREIGN KEY (order_id) 
        REFERENCES orders (order_id),
    CONSTRAINT fk_order_items_products 
        FOREIGN KEY (product_id) 
        REFERENCES products (product_id),
    CONSTRAINT fk_order_items_sellers 
        FOREIGN KEY (seller_id) 
        REFERENCES sellers (seller_id)
);

-- ==============================================================================
-- 3. INDEXING FOR PERFORMANCE
-- ==============================================================================
-- Index the timestamp column since the producer will constantly query it 
-- to slice time windows, and the downstream pipeline will query it for watermarks.
CREATE INDEX IF NOT EXISTS idx_orders_purchase_timestamp ON orders(order_purchase_timestamp);