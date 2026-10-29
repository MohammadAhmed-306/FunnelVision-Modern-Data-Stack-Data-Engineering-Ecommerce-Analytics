SELECT
    website_session_id,
    created_at,
    user_id,
    is_repeat_session,
    utm_source,
    utm_campaign,
    utm_content,
    device_type,
    http_referer
FROM {{ source('raw', 'website_sessions') }}
