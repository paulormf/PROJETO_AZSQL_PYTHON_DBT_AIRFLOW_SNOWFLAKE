WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'customer_contacts') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"contact_id"::VARCHAR, 19, 0) AS contact_id,
        TRY_TO_NUMBER(SOURCE_DATA:"customer_id"::VARCHAR, 19, 0) AS customer_id,
        NULLIF(TRIM(SOURCE_DATA:"contact_type"::VARCHAR), '') AS contact_type,
        NULLIF(TRIM(SOURCE_DATA:"contact_value"::VARCHAR), '') AS contact_value,
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
        PARTITION BY contact_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
