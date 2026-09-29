

-- models/gold/fact_payments.sql

select
    {{ dbt_utils.generate_surrogate_key([
        'p.order_id',
        'p.payment_sequential'
    ]) }} as payment_key,

    p.order_id,

    c.customer_key,

    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value

from {{ ref('silver_order_payments') }} p

left join {{ ref('silver_orders') }} o
    on p.order_id = o.order_id

left join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id

