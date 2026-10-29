SELECT
    product_id,
    product_name,
    created_at AS product_created_at,
    TO_DATE(created_at) AS product_created_date
FROM {{ ref('stg_products') }}
