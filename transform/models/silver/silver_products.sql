SELECT
    product_id::varchar(32),
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown')::varchar(50) AS product_category,
    p.product_name_lenght::int AS product_name_length,
    p.product_description_lenght::int AS product_description_length,
    p.product_photos_qty::int AS product_photos_qty,
    p.product_height_cm::int AS product_height_cm,
    p.product_length_cm::int AS product_length_cm,
    p.product_width_cm::int AS product_width_cm,
    p.product_weight_g::int AS product_weight_g
FROM {{ source('bronze', 'pg_products') }} p
LEFT JOIN {{ source('bronze', 'api_translations') }} t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL