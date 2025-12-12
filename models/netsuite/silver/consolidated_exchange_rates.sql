{{
    config(
        materialized="incremental",
        unique_key="CONSOLIDATED_EXCHANGE_RATE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "consolidated_exchange_rates") }}

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
            CAST(accounting_book_id AS INT) AS ACCOUNTING_BOOK_ID,
            CAST(accounting_period_id AS INT) AS POSTING_PERIOD_ID,
            CAST(average_rate AS INT) AS AVERAGE_RATE,
            CAST(consolidated_exchange_rate_id AS INT) AS CONSOLIDATED_EXCHANGE_RATE_ID,
            CAST(current_rate AS INT) AS CURRENT_RATE,
            CAST(from_subsidiary_id AS INT) AS FROM_SUBSIDIARY_ID,
            CAST(fromCurrency AS INT) AS FROM_CURRENCY_ID,
            CAST(historical_rate AS INT) AS HISTORICAL_RATE,
            CAST(to_subsidiary_id AS INT) AS TO_SUBSIDIARY_ID,
            CAST(toCurrency AS INT) AS TO_CURRENCY_ID,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
