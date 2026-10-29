
WITH all_dates AS (
    SELECT TO_DATE(created_at) AS date_day
    FROM {{ ref('stg_website_sessions') }}

    UNION

    SELECT TO_DATE(created_at)
    FROM {{ ref('stg_website_pageviews') }}

    UNION

    SELECT TO_DATE(created_at)
    FROM {{ ref('stg_orders') }}

    UNION

    SELECT TO_DATE(created_at)
    FROM {{ ref('stg_order_items') }}

    UNION

    SELECT TO_DATE(created_at)
    FROM {{ ref('stg_order_item_refunds') }}
)

SELECT
    date_day,
    YEAR(date_day) AS year,
    QUARTER(date_day) AS quarter,
    MONTH(date_day) AS month_number,
    MONTHNAME(date_day) AS month_name,
    DAY(date_day) AS day_of_month,
    DAYOFWEEKISO(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    WEEKOFYEAR(date_day) AS week_of_year
FROM all_dates
WHERE date_day IS NOT NULL
