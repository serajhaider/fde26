with deduped_reviews as (
    select
        *,
        row_number() over (
            partition by review_id, order_id
            order by review_creation_date desc nulls last
        ) as rn
    from {{ source('bronze', 'stream_order_reviews') }}
    where review_id is not null and order_id is not null
)

select
    dr.review_id::varchar(32) as review_id,
    dr.order_id::varchar(32) as order_id,
    dr.review_score::smallint as review_score,
    dr.review_comment_title::text as review_comment_title,
    dr.review_comment_message::text as review_comment_message,
    dr.review_creation_date::timestamp as review_creation_date,
    dr.review_answer_timestamp::timestamp as review_answer_timestamp
from deduped_reviews dr
inner join {{ ref('silver_orders') }} o
    on dr.order_id = o.order_id
where dr.rn = 1
order by dr.review_id, dr.order_id desc

