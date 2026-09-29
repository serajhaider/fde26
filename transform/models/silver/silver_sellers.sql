select
    seller_id::varchar(32),
    cast(seller_zip_code_prefix as numeric(5,0)) as seller_zip_code_prefix,
    initcap(seller_city)::varchar(100) as seller_city,
    upper(seller_state)::varchar(2) as seller_state
-- from {source('bronze', 'pg_sellers')}
from bronze.pg_sellers
where seller_id is not null