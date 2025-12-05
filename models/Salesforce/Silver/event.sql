{{
    config(
        materialized="incremental",
        unique_key="ACTIVITY_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "task_event") }}

    {% if is_incremental() %}
        WHERE CAST(LASTMODIFIEDDATE AS TIMESTAMP_NTZ) > (
            SELECT DATEADD(
                DAY,
                -1,
                COALESCE(MAX(LASTMODIFIEDDATE), '1900-01-01'::TIMESTAMP_NTZ)
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
    Id AS ACTIVITY_ID,
    OwnerId AS OWNER_USER_ID,
    WhoId AS WHO_ID,
    WhatId AS WHAT_ID,
    LASTMODIFIEDDATE,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
