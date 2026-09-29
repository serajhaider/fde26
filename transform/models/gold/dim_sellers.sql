--models/gold/dim_sellers.sql
select 
    {{dbt_utils.generate_surrogate_key(['s.seller_id'])}} as seller_key,
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat    as seller_lat,
    g.geolocation_lng    as seller_lng
from {{ ref('silver_sellers') }} s
left join {{ ref('silver_geolocation') }} g
    on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix

