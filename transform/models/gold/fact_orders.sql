-- models/gold/fact_orders.sql

select
    o.order_id,
    c.customer_key,
    o.order_status,

    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_ts

from {{ ref('silver_orders') }} o

left join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id

