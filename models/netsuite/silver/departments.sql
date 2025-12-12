{{
    config(
        materialized="incremental",
        unique_key="DEPARTMENT_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "departments") }}

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
            CAST(department_id AS INT) AS DEPARTMENT_ID,
            full_name AS DEPARTMENT_FULL_NAME,
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE,
            isinactive AS IS_INACTIVE,
            name AS DEPARTMENT_NAME,
            CAST(parent_id AS INT) AS PARENT,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
