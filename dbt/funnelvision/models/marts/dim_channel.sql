WITH channels AS (
    SELECT DISTINCT
        COALESCE(LOWER(TRIM(utm_source)), '(direct)') AS utm_source,
        COALESCE(LOWER(TRIM(utm_campaign)), '(none)') AS utm_campaign,
        COALESCE(LOWER(TRIM(utm_content)), '(none)') AS utm_content,
        COALESCE(LOWER(TRIM(device_type)), '(unknown)') AS device_type,
        COALESCE(LOWER(TRIM(http_referer)), '(none)') AS http_referer
    FROM {{ ref('stg_website_sessions') }}
)

SELECT
    MD5(
        CONCAT(
            utm_source, '|',
            utm_campaign, '|',
            utm_content, '|',
            device_type, '|',
            http_referer
        )
    ) AS channel_key,
    utm_source,
    utm_campaign,
    utm_content,
    device_type,
    http_referer
FROM channels
