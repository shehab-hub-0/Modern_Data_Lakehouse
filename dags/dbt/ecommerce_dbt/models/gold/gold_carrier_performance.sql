{{ config(
    materialized='table',
    tags=['gold', 'logistics']
) }}

SELECT
    carrier_name,
    shipping_method,
    COUNT(shipment_id) AS total_shipments,
    SUM(CASE WHEN shipping_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_shipments,
    SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END) AS delayed_shipments,
    AVG(delivery_days_taken) AS avg_delivery_days,
    AVG(shipping_cost) AS avg_shipping_cost,
    SUM(shipping_cost) AS total_shipping_cost,
    CASE 
        WHEN SUM(CASE WHEN shipping_status = 'delivered' THEN 1 ELSE 0 END) > 0 
        THEN (SUM(CASE WHEN is_delayed = TRUE THEN 1 ELSE 0 END) * 100.0) / SUM(CASE WHEN shipping_status = 'delivered' THEN 1 ELSE 0 END)
        ELSE 0 
    END AS delay_rate_pct
FROM {{ ref('silver_shipping_deliveries') }}
GROUP BY 1, 2