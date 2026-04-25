{{ config(
    materialized='table',
    tags=['gold', 'inventory']
) }}

SELECT
    snapshot_date,
    warehouse_id,
    COUNT(DISTINCT product_id) AS total_products,
    SUM(quantity_available) AS total_items_available,
    SUM(total_available_value) AS total_inventory_value,
    SUM(CASE WHEN stock_status = 'out_of_stock' THEN 1 ELSE 0 END) AS out_of_stock_products_count,
    SUM(CASE WHEN stock_status = 'low_stock' THEN 1 ELSE 0 END) AS low_stock_products_count
FROM {{ ref('silver_inventory_status') }}
GROUP BY 1, 2