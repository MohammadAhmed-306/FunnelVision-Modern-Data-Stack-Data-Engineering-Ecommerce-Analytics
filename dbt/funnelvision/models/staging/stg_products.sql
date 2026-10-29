SELECT
    product_id,
    created_at,
    product_name
FROM {{ source('raw', 'products') }}
