WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'order_items') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"order_item_id"::VARCHAR, 19, 0) AS order_item_id,
        TRY_TO_NUMBER(SOURCE_DATA:"order_id"::VARCHAR, 19, 0) AS order_id,
        TRY_TO_NUMBER(SOURCE_DATA:"product_id"::VARCHAR, 19, 0) AS product_id,
        TRY_TO_NUMBER(SOURCE_DATA:"quantity"::VARCHAR, 10, 0) AS quantity,
        TRY_TO_DECIMAL(SOURCE_DATA:"unit_price"::VARCHAR, 18, 2) AS unit_price,
        TRY_TO_DECIMAL(SOURCE_DATA:"discount"::VARCHAR, 18, 2) AS discount,
        TRY_TO_DECIMAL(SOURCE_DATA:"tax"::VARCHAR, 18, 2) AS tax,
        TRY_TO_DECIMAL(SOURCE_DATA:"total_item"::VARCHAR, 18, 2) AS total_item,
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
        PARTITION BY order_item_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
