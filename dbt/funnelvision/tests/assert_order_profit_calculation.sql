-- Business rule: profit must equal revenue minus cost.
-- The one-cent tolerance allows for decimal rounding differences.
-- Assumes revenue and cost should not be negative in this dataset.

SELECT *
FROM {{ ref('fct_orders') }}
WHERE
    profit IS NULL
    OR revenue IS NULL
    OR cost IS NULL
    OR ABS(profit - (revenue - cost)) > 0.01
    OR revenue < 0
    OR cost < 0
