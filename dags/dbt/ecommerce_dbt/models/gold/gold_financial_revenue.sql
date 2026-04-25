{{ config(
    materialized='table',
    tags=['gold', 'payments']
) }}

SELECT
    transaction_date,
    payment_method,
    country_code,
    COUNT(transaction_id) AS total_transactions,
    SUM(is_successful) AS successful_transactions,
    SUM(CASE WHEN is_successful = 1 THEN gross_amount ELSE 0 END) AS total_gross_revenue,
    SUM(CASE WHEN is_successful = 1 THEN gateway_fee ELSE 0 END) AS total_fees,
    SUM(CASE WHEN is_successful = 1 THEN net_amount ELSE 0 END) AS total_net_revenue,
    CASE 
        WHEN COUNT(transaction_id) > 0 THEN (SUM(is_successful) * 100.0) / COUNT(transaction_id) 
        ELSE 0 
    END AS success_rate_pct
FROM {{ ref('silver_payment_transactions') }}
GROUP BY 1, 2, 3