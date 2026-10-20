{{ config(materialized='view') }}

SELECT
    CUSTOMER_ID,
    CUSTOMER_TYPE,
    CASE
        WHEN CPF IS NULL OR TRIM(CPF) = '' THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(CPF, '[^0-9]', '')) = 11
            THEN '***.***.***-' ||
                 RIGHT(REGEXP_REPLACE(CPF, '[^0-9]', ''), 2)
        ELSE '***'
    END AS CPF_MASKED,
    STATUS AS CUSTOMER_STATUS,
    CREATED_AT,
    UPDATED_AT
FROM {{ ref('silver_customers') }}
