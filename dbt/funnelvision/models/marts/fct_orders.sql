SELECT
    order_id,
    created_at AS order_created_at,
    TO_DATE(created_at) AS order_date,
    website_session_id,
    user_id,
    primary_product_id,
    items_purchased,
    price_usd AS revenue,
    cogs_usd AS cost,
    price_usd - cogs_usd AS profit
FROM {{ ref('stg_orders') }}
