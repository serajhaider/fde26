-- models/gold/dim_customers.sql
select
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city   as customer_city,
    c.customer_state  as customer_state,
    g.geolocation_lat    as customer_lat,
    g.geolocation_lng    as customer_lng
from {{ ref('silver_customers') }} c
left join {{ ref('silver_geolocation') }} g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix

