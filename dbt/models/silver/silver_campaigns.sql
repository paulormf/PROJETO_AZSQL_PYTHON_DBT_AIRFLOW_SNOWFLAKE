WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'campaigns') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"campaign_id"::VARCHAR, 19, 0) AS campaign_id,
        NULLIF(TRIM(SOURCE_DATA:"campaign_name"::VARCHAR), '') AS campaign_name,
        NULLIF(TRIM(SOURCE_DATA:"channel"::VARCHAR), '') AS channel,
        TRY_TO_DATE(SOURCE_DATA:"start_date"::VARCHAR) AS start_date,
        TRY_TO_DATE(SOURCE_DATA:"end_date"::VARCHAR) AS end_date,
        TRY_TO_DECIMAL(SOURCE_DATA:"budget"::VARCHAR, 18, 2) AS budget,
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
        PARTITION BY campaign_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
