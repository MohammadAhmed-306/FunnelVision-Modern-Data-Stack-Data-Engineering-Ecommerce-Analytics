WITH refunds AS (
    SELECT
        order_item_id,
        SUM(refund_amount_usd) AS total_refunded
    FROM {{ ref('stg_order_item_refunds') }}
    GROUP BY order_item_id
)

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.created_at AS order_item_created_at,
    oi.is_primary_item,
    oi.price_usd,
    oi.cogs_usd,
    COALESCE(r.total_refunded, 0) AS total_refunded,
    oi.price_usd - COALESCE(r.total_refunded, 0) AS net_revenue,
    CASE
        WHEN COALESCE(r.total_refunded, 0) > 0 THEN 1
        ELSE 0
    END AS has_refund
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN refunds r
    ON oi.order_item_id = r.order_item_id
