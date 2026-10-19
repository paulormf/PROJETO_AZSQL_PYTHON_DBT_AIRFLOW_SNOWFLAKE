SELECT
    product_id, warehouse_id,
    COUNT(*) AS duplicate_count
FROM {{ ref('silver_inventory_balance') }}
GROUP BY product_id, warehouse_id
HAVING COUNT(*) > 1
