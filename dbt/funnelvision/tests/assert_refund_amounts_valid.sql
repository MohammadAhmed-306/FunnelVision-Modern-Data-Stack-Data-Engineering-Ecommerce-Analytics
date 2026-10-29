-- Business rule: refund amounts must not be negative.
-- This test does not impose a refund cap; confirm that rule before adding it.

SELECT *
FROM {{ ref('stg_order_item_refunds') }}
WHERE refund_amount_usd IS NULL
   OR refund_amount_usd < 0
