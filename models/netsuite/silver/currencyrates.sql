{{
    config(
        materialized="incremental",
        unique_key="CURRENCY_RATE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "currencyrates") }}

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
            CAST(currencyrate_id AS INT) AS CURRENCY_RATE_ID,
            CAST(date_effective AS DATE) AS EFFECTIVE_DATE, 
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE, 
            CAST(exchange_rate AS INT) AS EXCHANGE_RATE,
            externalId AS EXTERNALID,
            CAST(transactionCurrency AS INT) AS TRANSACTION_CURRENCY, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
