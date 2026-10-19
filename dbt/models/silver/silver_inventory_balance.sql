WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'inventory_balance') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"product_id"::VARCHAR, 19, 0) AS product_id,
        TRY_TO_NUMBER(SOURCE_DATA:"warehouse_id"::VARCHAR, 10, 0) AS warehouse_id,
        TRY_TO_NUMBER(SOURCE_DATA:"quantity_available"::VARCHAR, 10, 0) AS quantity_available,
        TRY_TO_NUMBER(SOURCE_DATA:"quantity_reserved"::VARCHAR, 10, 0) AS quantity_reserved,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"last_updated"::VARCHAR) AS last_updated,
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
        PARTITION BY product_id, warehouse_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
