WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'inventory_movements') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"movement_id"::VARCHAR, 19, 0) AS movement_id,
        TRY_TO_NUMBER(SOURCE_DATA:"product_id"::VARCHAR, 19, 0) AS product_id,
        TRY_TO_NUMBER(SOURCE_DATA:"warehouse_id"::VARCHAR, 10, 0) AS warehouse_id,
        NULLIF(TRIM(SOURCE_DATA:"movement_type"::VARCHAR), '') AS movement_type,
        TRY_TO_NUMBER(SOURCE_DATA:"quantity"::VARCHAR, 10, 0) AS quantity,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"movement_date"::VARCHAR) AS movement_date,
        NULLIF(TRIM(SOURCE_DATA:"reference_type"::VARCHAR), '') AS reference_type,
        TRY_TO_NUMBER(SOURCE_DATA:"reference_id"::VARCHAR, 19, 0) AS reference_id,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"created_at"::VARCHAR) AS created_at,
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
        PARTITION BY movement_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
