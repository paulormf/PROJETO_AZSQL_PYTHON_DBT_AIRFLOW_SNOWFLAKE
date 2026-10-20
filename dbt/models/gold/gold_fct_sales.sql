
SELECT
    -- Identificadores e grão da fato
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID,

    -- Data e dimensões para análise
    CAST(o.ORDER_DATE AS DATE) AS ORDER_DATE,
    o.CUSTOMER_ID,
    o.SALES_CHANNEL_ID,
    oi.PRODUCT_ID,

    -- Contexto do pedido
    o.STATUS AS ORDER_STATUS,
    o.PAYMENT_METHOD,

    -- Métricas do item
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.DISCOUNT AS ITEM_DISCOUNT,
    oi.TAX AS ITEM_TAX,
    oi.TOTAL_ITEM AS ITEM_TOTAL,

    -- Metadados de auditoria
    oi.CREATED_AT AS ITEM_CREATED_AT,
    oi.UPDATED_AT AS ITEM_UPDATED_AT

FROM {{ ref('silver_order_items') }} AS oi

INNER JOIN {{ ref('silver_orders') }} AS o
    ON oi.ORDER_ID = o.ORDER_ID
