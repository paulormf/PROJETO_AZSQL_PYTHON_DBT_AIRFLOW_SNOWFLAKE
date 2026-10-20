{{ config(materialized='table') }}

WITH sales AS (
    SELECT
        ORDER_ITEM_ID,
        ORDER_ID,
        ORDER_DATE,
        PRODUCT_ID,
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
),

classified AS (
    SELECT
        *,
        ROUND(ITEM_TOTAL - EXPECTED_TOTAL, 2) AS DIFFERENCE,

        ITEM_TOTAL < 0 AS IS_NEGATIVE_TOTAL,

        (
            ITEM_TOTAL IS NULL
            OR EXPECTED_TOTAL IS NULL
            OR ABS(ITEM_TOTAL - EXPECTED_TOTAL) > 0.02
        ) AS IS_FORMULA_MISMATCH,

        QUANTITY <= 0 AS IS_INVALID_QUANTITY

    FROM sales
)

SELECT
    *,
    CASE
        WHEN IS_FORMULA_MISMATCH
             AND IS_NEGATIVE_TOTAL
            THEN 'FORMULA_MISMATCH_AND_NEGATIVE_TOTAL'
        WHEN IS_FORMULA_MISMATCH
            THEN 'FORMULA_MISMATCH'
        WHEN IS_NEGATIVE_TOTAL
            THEN 'NEGATIVE_TOTAL'
        WHEN IS_INVALID_QUANTITY
            THEN 'INVALID_QUANTITY'
        ELSE 'OTHER'
    END AS ANOMALY_TYPE,

    CURRENT_TIMESTAMP() AS DIAGNOSED_AT

FROM classified
WHERE
    IS_FORMULA_MISMATCH
    OR IS_NEGATIVE_TOTAL
    OR IS_INVALID_QUANTITY
