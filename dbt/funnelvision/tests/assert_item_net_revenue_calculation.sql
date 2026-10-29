-- Business rule: item net revenue equals item price minus total refunds.
-- Assumes total_refunded is already aggregated per order item.

SELECT *
FROM {{ ref('int_order_items_with_refunds') }}
WHERE
    net_revenue IS NULL
    OR price_usd IS NULL
    OR total_refunded IS NULL
    OR ABS(net_revenue - (price_usd - total_refunded)) > 0.01
