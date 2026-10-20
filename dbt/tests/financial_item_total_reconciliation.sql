
{{ config(severity='warn') }}

WITH validation AS (
    SELECT
        ORDER_ITEM_ID,
        ORDER_ID,
        QUANTITY,
        UNIT_PRICE,
        ITEM_DISCOUNT,
        ITEM_TAX,
        ITEM_TOTAL,
        ROUND(
            QUANTITY * UNIT_PRICE
            - COALESCE(ITEM_DISCOUNT, 0)
            + COALESCE(ITEM_TAX, 0),
            2
        ) AS EXPECTED_TOTAL
    FROM {{ ref('gold_fct_sales') }}
)

SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    QUANTITY,
    UNIT_PRICE,
    ITEM_DISCOUNT,
    ITEM_TAX,
    ITEM_TOTAL,
    EXPECTED_TOTAL,
    ROUND(ITEM_TOTAL - EXPECTED_TOTAL, 2) AS DIFFERENCE
FROM validation
WHERE
    ITEM_TOTAL IS NULL
    OR EXPECTED_TOTAL IS NULL
    OR ABS(ITEM_TOTAL - EXPECTED_TOTAL) > 0.02
