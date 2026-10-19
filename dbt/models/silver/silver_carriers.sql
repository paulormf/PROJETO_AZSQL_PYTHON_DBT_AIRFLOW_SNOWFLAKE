WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'carriers') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"carrier_id"::VARCHAR, 10, 0) AS carrier_id,
        NULLIF(TRIM(SOURCE_DATA:"carrier_name"::VARCHAR), '') AS carrier_name,
        NULLIF(TRIM(SOURCE_DATA:"cnpj"::VARCHAR), '') AS cnpj,
        NULLIF(TRIM(SOURCE_DATA:"service_level"::VARCHAR), '') AS service_level,
        NULLIF(TRIM(SOURCE_DATA:"status"::VARCHAR), '') AS status,
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
        PARTITION BY carrier_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
