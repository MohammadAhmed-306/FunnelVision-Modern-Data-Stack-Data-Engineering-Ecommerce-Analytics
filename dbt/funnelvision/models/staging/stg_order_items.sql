SELECT
    order_item_id,
    created_at,
    order_id,
    product_id,
    is_primary_item,
    price_usd,
    cogs_usd
FROM {{ source('raw', 'order_items') }}
