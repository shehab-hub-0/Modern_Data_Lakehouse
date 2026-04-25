{{ config(
    materialized='table',
    tags=['silver', 'logistics']
) }}

SELECT
    shipment_id,
    order_id,
    tracking_number,
    carrier_name,
    shipping_method,
    shipping_status,
    shipped_at,
    estimated_delivery_at,
    actual_delivery_at,
    shipping_cost,
    weight_kg,
    is_delayed,
    DATE_DIFF('day', shipped_at, actual_delivery_at) AS delivery_days_taken,
    CASE 
        WHEN is_delayed = TRUE THEN DATE_DIFF('day', estimated_delivery_at, actual_delivery_at)
        ELSE 0 
    END AS delayed_by_days
FROM {{ ref('bronze_shipping_deliveries') }}