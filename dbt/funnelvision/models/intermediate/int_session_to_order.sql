
WITH sessions AS (
    SELECT *
    FROM {{ ref('stg_website_sessions') }}
),

orders AS (
    SELECT
        website_session_id,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(price_usd) AS session_revenue,
        MIN(created_at) AS first_order_at
    FROM {{ ref('stg_orders') }}
    GROUP BY website_session_id
)

SELECT
    s.website_session_id,
    s.created_at AS session_created_at,
    s.user_id,
    s.is_repeat_session,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type,
    s.http_referer,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.session_revenue, 0) AS session_revenue,
    o.first_order_at,
    CASE
        WHEN COALESCE(o.order_count, 0) > 0 THEN 1
        ELSE 0
    END AS converted_to_order
FROM sessions s
LEFT JOIN orders o
    ON s.website_session_id = o.website_session_id
