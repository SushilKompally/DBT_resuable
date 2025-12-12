{{
    config(
        materialized="incremental",
        unique_key="SUBSIDIARY_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "subsidiaries") }}

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
            CAST(base_currency_id AS INT) AS "CURRENCY_ID", 
            full_name AS "SUBSIDIARY_FULL_NAME", 
            isinactive AS "IS_INACTIVE", 
            name AS "SUBSIDIARY_NAME", 
            CAST(parent_id AS INT) AS "PARENT_ID", 
            CAST(subsidiary_id AS INT) AS "SUBSIDIARY_ID", 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
