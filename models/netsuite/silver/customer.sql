{{
    config(
        materialized="incremental",
        unique_key="CUSTOMER_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "customer") }}

    {% if is_incremental() %}
        WHERE CAST(LASTMODIFIEDDATE AS TIMESTAMP_NTZ) > (
            SELECT DATEADD(
                DAY,
                -1,
                COALESCE(MAX(LAST_MODIFIED_DATE), '1900-01-01'::TIMESTAMP_NTZ)
            )
            FROM {{ this }}
        )
        AND 1 = 1
    {% else %}
        WHERE 1 = 1
    {% endif %}
    ),

    CLEANED AS (
          SELECT
            CAST(customer_id AS NUMBER) AS CUSTOMER_ID,
            entity_id AS ENTITY_ID,
            CUSTOMER_NAME,
            email AS EMAIL,
            phone AS PHONE,
            status AS STATUS,
            CAST(date_created AS TIMESTAMP) AS DATE_CREATED,
            CAST(last_modified_date AS TIMESTAMP) AS LAST_MODIFIED_DATE,
            is_inactive AS IS_INACTIVE,
            currency AS CURRENCY,
            terms AS TERMS,
            CAST(sales_rep_id AS NUMBER) AS SALES_REP_ID,
            CAST(parent_customer_id AS NUMBER) AS PARENT_CUSTOMER_ID,
            billing_address AS BILLING_ADDRESS,
            shipping_address AS SHIPPING_ADDRESS,
            CAST(credit_limit AS NUMBER(18,2)) AS CREDIT_LIMIT, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
