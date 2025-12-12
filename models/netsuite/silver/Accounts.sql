{{
    config(
        materialized="incremental",
        unique_key="ACCOUNT_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "accounts") }}

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
            CAST(account_id AS INT) AS ACCOUNT_ID,
            accountnumber AS ACCOUNT_NUMBER,
            CAST(class_id AS INT) AS CLASS_ID,
            CAST(currency_id AS INT) AS CURRENCY_ID,
            CAST(date_last_modified AS DATE) AS LAST_MODIFIED_DATE,
            CAST(department_id AS INT) AS DEPARTMENT_ID,
            description AS ACCOUNT_DESCRIPTION,
            full_description AS DISPLAY_NAME,
            full_name AS DISPLAY_NAME_WITH_HIERARCHY,
            isinactive AS IS_INACTIVE,
            CAST(location_id AS INT) AS LOCATION_ID,
            name AS ACCOUNT_NAME,
            CAST(parent_id AS INT) AS PARENT_ID,
            CAST(subsidiary AS INT) AS SUBSIDIARY_ID,
            type_name AS ACCOUNT_TYPE,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
