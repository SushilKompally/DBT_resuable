{{
    config(
        materialized="incremental",
        unique_key="TRANSACTION_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "transactions") }}

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
            CAST(accounting_period_id AS INT) AS POSTING_PERIOD_ID, 
            CAST(amount_unbilled AS INT) AS AMOUNT_UNBILLED, 
            billaddress AS BILLING_ADDRESS_ID, 
            CAST(closed AS DATE) AS CLOSE_DATE, 
            CAST(company_status_id AS INT) AS TRANSACTION_STATUS_ID, 
            CAST(create_date AS DATE) AS CREATED_DATE, 
            CAST(created_by_id AS INT) AS EMPLOYEE_ID, 
            CAST(created_from_id AS INT) AS SOURCE_TRANSACTION, 
            CAST(currency_id AS INT) AS CURRENCY_ID, 
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE, 
            CAST(due_date AS DATE) AS DUE_DATE, 
            email AS SESSION_SHOP_EMAIL, 
            CAST(end_date AS DATE) AS END_DATE, 
            CAST(ENTITY_id AS INT) AS INTERNAL_ENTITY_ID, 
            CAST(exchange_rate AS INT) AS EXCHANGE_RATE, 
            CAST(location_id AS INT) AS LOCATION_ID, 
            memo AS MEMO, 
            CAST(partner_id AS INT) AS PARTNER_ID, 
            CAST(payment_terms_id AS INT) AS PAYMENT_METHOD_ID, 
            shipaddress AS SHIPPING_ADDRESS_ID, 
            CAST(start_date AS DATE) AS START_DATE, 
            status AS BILLING_STATUS, 
            title AS TITLE, 
            CAST(trandate AS DATE) AS TRAN_DATE, 
            tranid AS TRAN_ID, 
            CAST(transaction_id AS INT) AS TRANSACTION_ID, 
            transaction_number AS TRANSACTION_NUMBER, 
            transaction_partner AS TRANSACTION_SESSION_RO, 
            transaction_SOURCE AS SOURCE, 
            transaction_type AS TRANSACTION_TYPE, 
            CAST(transaction_website AS INT) AS TRANSACTION_SESSION_VIN, 
            RECORD_TYPE,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
