WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'products') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"product_id"::VARCHAR, 19, 0) AS product_id,
        NULLIF(TRIM(SOURCE_DATA:"sku"::VARCHAR), '') AS sku,
        NULLIF(TRIM(SOURCE_DATA:"product_name"::VARCHAR), '') AS product_name,
        TRY_TO_NUMBER(SOURCE_DATA:"category_id"::VARCHAR, 10, 0) AS category_id,
        TRY_TO_NUMBER(SOURCE_DATA:"brand_id"::VARCHAR, 10, 0) AS brand_id,
        TRY_TO_NUMBER(SOURCE_DATA:"supplier_id"::VARCHAR, 10, 0) AS supplier_id,
        NULLIF(TRIM(SOURCE_DATA:"unit"::VARCHAR), '') AS unit,
        TRY_TO_DECIMAL(SOURCE_DATA:"weight"::VARCHAR, 18, 3) AS weight,
        TRY_TO_DECIMAL(SOURCE_DATA:"cost_price"::VARCHAR, 18, 2) AS cost_price,
        TRY_TO_DECIMAL(SOURCE_DATA:"sale_price"::VARCHAR, 18, 2) AS sale_price,
        TRY_TO_DECIMAL(SOURCE_DATA:"tax_rate"::VARCHAR, 8, 4) AS tax_rate,
        NULLIF(TRIM(SOURCE_DATA:"status"::VARCHAR), '') AS status,
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
        PARTITION BY product_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
