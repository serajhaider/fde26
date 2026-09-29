

-- models/gold/dim_products.sql
select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    initcap(replace(product_category,'_',' ')) as product_category,
    product_name_length,
    product_description_length,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty
from {{ ref('silver_products') }}

