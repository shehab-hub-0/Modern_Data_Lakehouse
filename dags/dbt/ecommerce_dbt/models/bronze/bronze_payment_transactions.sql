{{ config(
    materialized='table',
    tags=['bronze', 'payments']
) }}

WITH raw_payments AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY transaction_timestamp DESC
        ) as row_num
    FROM {{ ref('raw_payment_transactions') }}
)

SELECT
    TRIM(CAST(transaction_id AS VARCHAR)) AS transaction_id,
    TRIM(CAST(order_id AS VARCHAR)) AS order_id,
    TRIM(CAST(customer_id AS VARCHAR)) AS customer_id,
    TRIM(CAST(merchant_id AS VARCHAR)) AS merchant_id,

    LOWER(TRIM(CAST(payment_method AS VARCHAR))) AS payment_method,
    LOWER(TRIM(CAST(payment_status AS VARCHAR))) AS payment_status,
    TRIM(CAST(processor_response_code AS VARCHAR)) AS response_code,

    ABS(CAST(amount AS DECIMAL(18, 2))) AS gross_amount,
    ABS(CAST(gateway_fee AS DECIMAL(18, 2))) AS gateway_fee,
    
    (ABS(CAST(amount AS DECIMAL(18, 2))) - ABS(CAST(gateway_fee AS DECIMAL(18, 2)))) AS net_amount,

    UPPER(TRIM(CAST(currency AS VARCHAR))) AS currency_code,
    UPPER(TRIM(CAST(billing_country AS VARCHAR))) AS country_code,

    CAST(transaction_timestamp AS TIMESTAMP) AS transaction_at,

    CAST(risk_score AS INTEGER) AS risk_score,
    CASE 
        WHEN CAST(risk_score AS INTEGER) >= 80 THEN 'high_risk'
        WHEN CAST(risk_score AS INTEGER) >= 40 THEN 'medium_risk'
        ELSE 'low_risk'
    END AS risk_category,

    CURRENT_TIMESTAMP AS loaded_at,
    'payment_gateway_alpha' AS source_system

FROM raw_payments
WHERE row_num = 1