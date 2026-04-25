{{ config(
    materialized='table',
    tags=['silver', 'events']
) }}

SELECT
    session_id,
    MAX(customer_id) AS customer_id,
    MIN(event_timestamp) AS session_start_at,
    MAX(event_timestamp) AS session_end_at,
    COUNT(event_id) AS total_events_count,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS is_conversion,
    MAX(referrer_source) AS acquisition_channel,
    MAX(device_type) AS device_type,
    MAX(ip_address) AS ip_address
FROM {{ ref('bronze_customer_events') }}
GROUP BY session_id