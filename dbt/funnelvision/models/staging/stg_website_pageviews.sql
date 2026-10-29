SELECT
    website_pageview_id,
    created_at,
    website_session_id,
    pageview_url
FROM {{ source('raw', 'website_pageviews') }}
