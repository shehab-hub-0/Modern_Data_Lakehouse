{{ config(
    materialized='table',
    tags=['gold', 'events']
) }}

SELECT
    CAST(session_start_at AS DATE) AS session_date,
    acquisition_channel,
    device_type,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(is_conversion) AS total_conversions,
    CASE 
        WHEN COUNT(DISTINCT session_id) > 0 THEN (SUM(is_conversion) * 100.0) / COUNT(DISTINCT session_id) 
        ELSE 0 
    END AS conversion_rate_pct
FROM {{ ref('silver_customer_sessions') }}
GROUP BY 1, 2, 3