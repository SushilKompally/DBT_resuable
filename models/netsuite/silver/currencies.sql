{{
    config(
        materialized="incremental",
        unique_key="CURRENCY_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "currencies") }}

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
            CAST(currency_id AS INT) AS CURRENCY_ID,
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE,
            CAST(exchangeRate AS INT) AS EXCHANGE_RATE,
            is_inactive AS IS_INACTIVE,
            isBaseCurrency AS IS_BASE_CURRENCY,
            name AS CURRENCY_NAME,
            symbol AS DISPLAY_SYMBOL,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
