WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'payment_transactions') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"transaction_id"::VARCHAR, 19, 0) AS transaction_id,
        TRY_TO_NUMBER(SOURCE_DATA:"payment_id"::VARCHAR, 19, 0) AS payment_id,
        NULLIF(TRIM(SOURCE_DATA:"gateway"::VARCHAR), '') AS gateway,
        NULLIF(TRIM(SOURCE_DATA:"authorization_code"::VARCHAR), '') AS authorization_code,
        NULLIF(TRIM(SOURCE_DATA:"transaction_status"::VARCHAR), '') AS transaction_status,
        TRY_TO_DECIMAL(SOURCE_DATA:"transaction_amount"::VARCHAR, 18, 2) AS transaction_amount,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"transaction_date"::VARCHAR) AS transaction_date,
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
        PARTITION BY transaction_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
