

-- models/gold/fact_reviews.sql
with deduped_reviews as (
    select
        *,
        row_number() over (
            partition by review_id
            order by review_creation_date desc nulls last, order_id desc
        ) as rn
    from {{ ref('silver_order_reviews') }}
)

select
    r.review_id,
    r.order_id,
    c.customer_key,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp
from deduped_reviews r
inner join {{ ref('silver_orders') }} o
    on r.order_id = o.order_id
inner join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id
where r.rn = 1

