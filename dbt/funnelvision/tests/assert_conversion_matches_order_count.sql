-- Business rule: sessions with at least one order must be marked as converted.
-- The test passes when this query returns zero rows.

SELECT *
FROM {{ ref('int_session_to_order') }}
WHERE
    (order_count > 0 AND converted_to_order != 1)
    OR
    (order_count = 0 AND converted_to_order != 0)
    OR order_count IS NULL
    OR converted_to_order IS NULL
