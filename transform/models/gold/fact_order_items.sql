
-- models/gold/fact_order_items.sql

select
    {{ dbt_utils.generate_surrogate_key([
        'oi.order_id',
        'oi.order_item_id'
    ]) }} as order_item_key,

    oi.order_id,

    c.customer_key,
    p.product_key,
    s.seller_key,

    oi.order_item_id,

    oi.shipping_limit_ts,
    oi.price,
    oi.freight_value,
    oi.item_total_value

from {{ ref('silver_order_items') }} oi

inner join {{ ref('silver_orders') }} o
    on oi.order_id = o.order_id

inner join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id

inner join {{ ref('dim_products') }} p
    on oi.product_id = p.product_id

inner join {{ ref('dim_sellers') }} s
    on oi.seller_id = s.seller_id

