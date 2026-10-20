SELECT
    p.PRODUCT_ID,
    p.SKU,
    p.PRODUCT_NAME,
    p.CATEGORY_ID,
    c.CATEGORY_NAME,
    p.BRAND_ID,
    b.BRAND_NAME,
    p.SUPPLIER_ID,
    s.COMPANY_NAME AS SUPPLIER_NAME,
    p.UNIT,
    p.WEIGHT,
    p.COST_PRICE,
    p.SALE_PRICE,
    p.TAX_RATE,
    p.STATUS AS PRODUCT_STATUS,
    p.CREATED_AT,
    p.UPDATED_AT
FROM {{ ref('silver_products') }} AS p
LEFT JOIN {{ ref('silver_categories') }} AS c
    ON p.CATEGORY_ID = c.CATEGORY_ID
LEFT JOIN {{ ref('silver_brands') }} AS b
    ON p.BRAND_ID = b.BRAND_ID
LEFT JOIN {{ ref('silver_suppliers') }} AS s
    ON p.SUPPLIER_ID = s.SUPPLIER_ID