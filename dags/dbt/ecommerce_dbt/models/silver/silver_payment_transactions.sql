{{ config(
    materialized='table',
    tags=['silver', 'payments']
) }}

SELECT
    transaction_id,
    order_id,
    customer_id,
    merchant_id,
    payment_method,
    payment_status,
    response_code,
    gross_amount,
    gateway_fee,
    net_amount,
    currency_code,
    country_code,
    transaction_at,
    CAST(transaction_at AS DATE) AS transaction_date,
    risk_score,
    risk_category,
    CASE WHEN payment_status = 'completed' THEN 1 ELSE 0 END AS is_successful
FROM {{ ref('bronze_payment_transactions') }}