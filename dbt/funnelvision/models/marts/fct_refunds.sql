SELECT
    r.order_item_refund_id,
    r.created_at AS refund_created_at,
    TO_DATE(r.created_at) AS refund_date,
    r.order_item_id,
    r.order_id,
    i.product_id,
    r.refund_amount_usd,
    i.price_usd AS order_item_price,
    i.total_refunded AS total_refunded_for_item,
    i.net_revenue AS item_net_revenue
FROM {{ ref('stg_order_item_refunds') }} r
INNER JOIN {{ ref('int_order_items_with_refunds') }} i
    ON r.order_item_id = i.order_item_id
