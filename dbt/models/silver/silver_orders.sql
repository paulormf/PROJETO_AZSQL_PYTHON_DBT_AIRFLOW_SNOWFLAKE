WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'orders') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"order_id"::VARCHAR, 19, 0) AS order_id,
        TRY_TO_NUMBER(SOURCE_DATA:"customer_id"::VARCHAR, 19, 0) AS customer_id,
        TRY_TO_NUMBER(SOURCE_DATA:"sales_channel_id"::VARCHAR, 10, 0) AS sales_channel_id,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"order_date"::VARCHAR) AS order_date,
        NULLIF(TRIM(SOURCE_DATA:"status"::VARCHAR), '') AS status,
        NULLIF(TRIM(SOURCE_DATA:"payment_method"::VARCHAR), '') AS payment_method,
        TRY_TO_NUMBER(SOURCE_DATA:"shipping_address_id"::VARCHAR, 19, 0) AS shipping_address_id,
        TRY_TO_DECIMAL(SOURCE_DATA:"subtotal"::VARCHAR, 18, 2) AS subtotal,
        TRY_TO_DECIMAL(SOURCE_DATA:"discount"::VARCHAR, 18, 2) AS discount,
        TRY_TO_DECIMAL(SOURCE_DATA:"shipping_cost"::VARCHAR, 18, 2) AS shipping_cost,
        TRY_TO_DECIMAL(SOURCE_DATA:"tax"::VARCHAR, 18, 2) AS tax,
        TRY_TO_DECIMAL(SOURCE_DATA:"total_amount"::VARCHAR, 18, 2) AS total_amount,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"created_at"::VARCHAR) AS created_at,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"updated_at"::VARCHAR) AS updated_at,
        SOURCE_KEY AS source_key,
        ROW_HASH AS row_hash,
        SOURCE_UPDATED_AT AS source_updated_at,
        INGESTED_AT AS ingested_at,
        RUN_ID AS run_id
    FROM source_data

),

deduplicated AS (

    SELECT *
    FROM typed_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
