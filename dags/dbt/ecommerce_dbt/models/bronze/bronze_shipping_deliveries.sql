{{ config(
    materialized='table',
    tags=['bronze', 'logistics']
) }}

WITH raw_shipping AS (
    SELECT 
        *,
        -- التخلص من التكرار بناءً على رقم الشحنة (Tracking Number) أو المعرف الفريد
        ROW_NUMBER() OVER (
            PARTITION BY tracking_number 
            ORDER BY shipping_date DESC
        ) as row_num
    FROM {{ ref('row_support_tickets') }} -- ملاحظة: الملف اسمه في الكود الخاص بك row_support_tickets رغم أنه بيانات شحن
)

SELECT
    -- 1. تنظيف المعرفات وأرقام التتبع
    TRIM(shipment_id) AS shipment_id,
    TRIM(order_id) AS order_id,
    UPPER(TRIM(tracking_number)) AS tracking_number,
    TRIM(shipping_address_id) AS shipping_address_id,

    -- 2. توحيد مسميات الشركات وطرق الشحن
    LOWER(TRIM(carrier_name)) AS carrier_name, -- يحولها لـ FedEx, Ups...
    LOWER(TRIM(shipping_method)) AS shipping_method,
    LOWER(TRIM(shipping_status)) AS shipping_status,

    -- 3. معالجة التواريخ والتحقق من المنطق (Dates Validation)
    CAST(shipping_date AS TIMESTAMP) AS shipped_at,
    CAST(estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at,
    CAST(actual_delivery_date AS TIMESTAMP) AS actual_delivery_at,

    -- 4. معالجة البيانات الرقمية (Costs & Weights)
    ABS(CAST(shipping_cost AS DECIMAL(10, 2))) AS shipping_cost,
    ABS(CAST(weight_kg AS DECIMAL(10, 2))) AS weight_kg,

    -- 5. إضافة منطق ذكاء الأعمال (Calculated Fields)
    -- حساب هل الشحنة تأخرت أم لا
    CASE 
        WHEN actual_delivery_date IS NOT NULL AND actual_delivery_date > estimated_delivery_date THEN TRUE
        ELSE FALSE 
    END AS is_delayed,

    -- 6. أعمدة التدقيق
    CURRENT_TIMESTAMP AS loaded_at,
    'logistics_system_01' AS source_system

FROM raw_shipping
