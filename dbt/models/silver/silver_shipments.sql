WITH source_data AS (

    SELECT
        SOURCE_KEY,
        ROW_HASH,
        SOURCE_UPDATED_AT,
        INGESTED_AT,
        RUN_ID,
        SOURCE_DATA
    FROM {{ source('bronze', 'shipments') }}

),

typed_data AS (

    SELECT
        TRY_TO_NUMBER(SOURCE_DATA:"shipment_id"::VARCHAR, 19, 0) AS shipment_id,
        TRY_TO_NUMBER(SOURCE_DATA:"order_id"::VARCHAR, 19, 0) AS order_id,
        TRY_TO_NUMBER(SOURCE_DATA:"carrier_id"::VARCHAR, 10, 0) AS carrier_id,
        NULLIF(TRIM(SOURCE_DATA:"tracking_code"::VARCHAR), '') AS tracking_code,
        TRY_TO_TIMESTAMP_NTZ(SOURCE_DATA:"shipment_date"::VARCHAR) AS shipment_date,
        TRY_TO_DATE(SOURCE_DATA:"estimated_delivery_date"::VARCHAR) AS estimated_delivery_date,
        TRY_TO_DATE(SOURCE_DATA:"actual_delivery_date"::VARCHAR) AS actual_delivery_date,
        NULLIF(TRIM(SOURCE_DATA:"status"::VARCHAR), '') AS status,
        TRY_TO_DECIMAL(SOURCE_DATA:"shipping_cost"::VARCHAR, 18, 2) AS shipping_cost,
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
        PARTITION BY shipment_id
        ORDER BY
            source_updated_at DESC NULLS LAST,
            ingested_at DESC NULLS LAST,
            run_id DESC NULLS LAST,
            row_hash DESC
    ) = 1

)

SELECT *
FROM deduplicated
