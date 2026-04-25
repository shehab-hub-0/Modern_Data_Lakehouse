{{ config(
    materialized='table',
    tags=['silver', 'inventory']
) }}

SELECT
    snapshot_id,
    product_id,
    warehouse_id,
    supplier_id,
    snapshot_date,
    quantity_on_hand,
    quantity_reserved,
    quantity_available_calculated AS quantity_available,
    reorder_point,
    reorder_quantity,
    unit_cost,
    (quantity_available_calculated * unit_cost) AS total_available_value,
    is_below_reorder_level,
    CASE 
        WHEN quantity_available_calculated <= 0 THEN 'out_of_stock'
        WHEN is_below_reorder_level THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status
FROM {{ ref('bronze_inventory_snapshots') }}