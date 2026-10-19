WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'customer_addresses') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"address_id"::VARCHAR, 19, 0) AS address_id,
        TRY_TO_NUMBER(SOURCE_DATA:"customer_id"::VARCHAR, 19, 0) AS customer_id,
        NULLIF(TRIM(SOURCE_DATA:"address_type"::VARCHAR), '') AS address_type,
        NULLIF(TRIM(SOURCE_DATA:"street"::VARCHAR), '') AS street,
        NULLIF(TRIM(SOURCE_DATA:"number"::VARCHAR), '') AS number,
        NULLIF(TRIM(SOURCE_DATA:"complement"::VARCHAR), '') AS complement,
        NULLIF(TRIM(SOURCE_DATA:"neighborhood"::VARCHAR), '') AS neighborhood,
        NULLIF(TRIM(SOURCE_DATA:"city"::VARCHAR), '') AS city,
        NULLIF(TRIM(SOURCE_DATA:"state"::VARCHAR), '') AS state,
        NULLIF(TRIM(SOURCE_DATA:"zip_code"::VARCHAR), '') AS zip_code,
        NULLIF(TRIM(SOURCE_DATA:"country"::VARCHAR), '') AS country,
        TRY_TO_BOOLEAN(SOURCE_DATA:"is_primary"::VARCHAR) AS is_primary,
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
        PARTITION BY address_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
