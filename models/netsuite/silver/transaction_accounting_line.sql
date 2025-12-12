{{
    config(
        materialized="incremental",
        unique_key="TRANSACTION_LINE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "transaction_accounting_line") }}

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
            CAST(account AS INT) AS ACCOUNT_ID, 
            CAST(accountingBook AS INT) AS ACCOUNTING_BOOK_ID, 
            accountType AS ACCOUNT_TYPE, 
            CAST(amount AS INT) AS AMOUNT, 
            CAST(amountPaid AS INT) AS AMOUNT_PAID, 
            CAST(amountUnpaid AS INT) AS AMOUNT_UN_PAID, 
            CAST(exchangeRate AS INT) AS EXCHANGE_RATE, 
            CAST(lastModifiedDate AS DATE) AS LAST_MODIFIED_DATE, 
            CAST(netAmount AS INT) AS NET_AMOUNT, 
            posting AS TRANSACTION_ACCOUNTING_POSTING_FLAG, 
            CAST(transaction AS INT) AS TRANSACTION_ID, 
            CAST(transactionLine AS INT) AS TRANSACTION_LINE_ID, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
