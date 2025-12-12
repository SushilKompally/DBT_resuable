{{
    config(
        materialized="incremental",
        unique_key="TRANSACTION_STATUS_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "transaction_status") }}

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
            CAST(TRANSACTION_STATUS_ID AS NUMBER) AS TRANSACTION_STATUS_ID,
            TRANSACTION_STATUS_FULL_NAME,
            TRANSACTION_STATUS_NAME,
            TRAN_CUSTOM_TYPE_ID,
            TRANSACTION_TYPE,
            INGESTION_TIME,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
