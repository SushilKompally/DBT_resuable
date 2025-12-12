{{
    config(
        materialized="incremental",
        unique_key="POATING_PERIOD_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "accounting_periods") }}

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
            CAST(closed_on AS DATE) AS CLOSED_ON_DATE, 
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE, 
            CAST(ending AS DATE) AS END_DATE, 
            full_name AS PERIOD_NAME, 
            CAST(starting AS DATE) AS START_DATE, 
            year_0 AS YEAR, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
