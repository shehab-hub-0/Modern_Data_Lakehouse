{{ config(
    materialized='table',
    tags=['bronze', 'inventory']
) }}

WITH raw_inventory AS (
    SELECT 
        *,
        -- التخلص من أي سجلات مكررة لنفس المنتج في نفس المخزن في نفس اليوم
        ROW_NUMBER() OVER (
            PARTITION BY product_id, warehouse_id, snapshot_date 
            ORDER BY snapshot_id DESC
        ) as row_num
    FROM {{ ref('raw_inventory_snapshots') }}
)

SELECT
    -- 1. تنظيف المعرفات (Primary Keys & Foreign Keys)
    TRIM(snapshot_id) AS snapshot_id,
    TRIM(product_id) AS product_id,
    TRIM(warehouse_id) AS warehouse_id,
    TRIM(supplier_id) AS supplier_id,

    -- 2. معالجة التواريخ (Casting & Validation)
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(last_received_date AS DATE) AS last_received_date,

    -- 3. تصحيح الأخطاء الحسابية (Data Integrity)
    -- نضمن أن الكمية المتاحة هي دائماً الفرق، حتى لو كان الملف الأصلي يحتوي على خطأ
    CAST(quantity_on_hand AS INT) AS quantity_on_hand,
    CAST(quantity_reserved AS INT) AS quantity_reserved,
    (CAST(quantity_on_hand AS INT) - CAST(quantity_reserved AS INT)) AS quantity_available_calculated,
    
    -- 4. التعامل مع القيم المتطرفة أو السالبة (Negative Values)
    GREATEST(CAST(reorder_point AS INT), 0) AS reorder_point,
    GREATEST(CAST(reorder_quantity AS INT), 0) AS reorder_quantity,
    
    -- 5. معالجة التكلفة (Financial Data)
    ABS(CAST(unit_cost AS DECIMAL(10, 2))) AS unit_cost,

    -- 6. إضافة أعمدة التدقيق (Metadata)
    CURRENT_TIMESTAMP AS loaded_at,
    'inventory_system' AS source_system,
    
    -- إضافة علم (Flag) إذا كان المخزون وصل لنقطة إعادة الطلب
    CASE 
        WHEN (quantity_on_hand - quantity_reserved) <= reorder_point THEN TRUE 
        ELSE FALSE 
    END AS is_below_reorder_level

FROM raw_inventory
