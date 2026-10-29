SELECT
    website_session_id,
    session_created_at,
    user_id,
    is_repeat_session,
    utm_source,
    utm_campaign,
    utm_content,
    device_type,
    http_referer,
    order_count,
    session_revenue,
    first_order_at,
    converted_to_order
FROM {{ ref('int_session_to_order') }}
