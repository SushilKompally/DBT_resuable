{{
    config(
        materialized="incremental",
        unique_key="CLASS_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "classification") }}

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
            fullname AS CLASS_FULL_NAME, 
            CAST(id AS INT) AS CLASS_ID, 
            isinactive AS IS_INACTIVE, 
            CAST(lastmodifieddate AS DATE) AS LAST_MODIFIED_DATE, 
            name AS CLASS_NAME, 
            CAST(parent AS INT) AS PARENT, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
