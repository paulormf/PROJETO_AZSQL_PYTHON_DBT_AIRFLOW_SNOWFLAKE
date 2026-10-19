WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'suppliers') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"supplier_id"::VARCHAR, 10, 0) AS supplier_id,
        NULLIF(TRIM(SOURCE_DATA:"company_name"::VARCHAR), '') AS company_name,
        NULLIF(TRIM(SOURCE_DATA:"trade_name"::VARCHAR), '') AS trade_name,
        NULLIF(TRIM(SOURCE_DATA:"cnpj"::VARCHAR), '') AS cnpj,
        NULLIF(TRIM(SOURCE_DATA:"email"::VARCHAR), '') AS email,
        NULLIF(TRIM(SOURCE_DATA:"phone"::VARCHAR), '') AS phone,
        NULLIF(TRIM(SOURCE_DATA:"city"::VARCHAR), '') AS city,
        NULLIF(TRIM(SOURCE_DATA:"state"::VARCHAR), '') AS state,
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
        PARTITION BY supplier_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
