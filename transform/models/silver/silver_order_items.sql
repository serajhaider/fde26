select distinct on (order_id,order_item_id)
    order_id::varchar(32),
    order_item_id::int,
    product_id::varchar(32),
    seller_id::varchar(32),
    shipping_limit_date::timestamp as shipping_limit_ts,
    price::numeric(10,2),
    freight_value::numeric(10,2),
    (price + freight_value)::numeric(10,2) as item_total_value
from {{ source('bronze', 'pg_order_items') }}
where order_id is not null
  and order_item_id is not null
order by order_id, order_item_id 