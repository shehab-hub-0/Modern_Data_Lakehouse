{{ config(
    materialized='table',
    tags=['bronze']
) }}

-- استخدام CTE لترتيب البيانات والتعامل مع التكرار
WITH raw_data AS (
    SELECT 
        *,
        -- التعامل مع التكرار: ترتيب السجلات المتشابهة حسب الوقت (الأحدث أولاً)
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY event_timestamp DESC
        ) as row_num
    FROM {{ ref('raw_customer_events') }}
)

SELECT
    -- 1. توحيد المعرفات وتنظيفها
    TRIM(event_id) AS event_id,
    TRIM(customer_id) AS customer_id,
    TRIM(session_id) AS session_id,
    
    -- 2. توحيد حالة الأحرف (Case Sensitivity) لمنع تكرار التصنيفات
    LOWER(TRIM(event_type)) AS event_type,
    
    -- 3. معالجة التواريخ والتأكد من أنها منطقية
    CASE 
        WHEN CAST(event_timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP
        ELSE CAST(event_timestamp AS TIMESTAMP)
    END AS event_timestamp,
    
    -- 4. تنظيف الروابط والتعامل مع القيم المفقودة
    LOWER(TRIM(page_url)) AS page_url,
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(category_id), '') AS category_id,
    
    -- 5. معالجة بيانات المصدر والجهاز
    COALESCE(LOWER(TRIM(referrer_source)), 'direct') AS referrer_source,
    COALESCE(LOWER(TRIM(device_type)), 'unknown') AS device_type,
    
    user_agent,
    ip_address,

    -- 6. إضافة بيانات التتبع والتدقيق
    CURRENT_TIMESTAMP AS loaded_at,
    'raw_customer_events' AS source_system,
    {{ dbt_utils.generate_surrogate_key(['event_id', 'event_timestamp']) }} AS dbt_updated_key

FROM raw_data
