select distinct on (order_id)
    order_id::varchar(32) as order_id,
    customer_id::varchar(32),
    order_status::varchar(20),
    order_purchase_timestamp::timestamp        as order_purchase_ts,
    order_approved_at::timestamp                as order_approved_ts,
    order_delivered_carrier_date::timestamp     as order_delivered_carrier_ts,
    order_delivered_customer_date::timestamp    as order_delivered_customer_ts,
    order_estimated_delivery_date::timestamp    as order_estimated_delivery_ts
from {{ source('bronze', 'pg_orders') }}
where order_id is not null
order by order_id,order_purchase_timestamp desc