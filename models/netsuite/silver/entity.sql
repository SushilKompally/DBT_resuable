{{
    config(
        materialized="incremental",
        unique_key="INTERNAL_ENTITY_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "entity") }}

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
             CAST(contact AS INT) AS CONTACT_ID, 
            CAST(customer AS INT) AS CUSTOMER_ID, 
            CAST(datecreated AS DATE) AS DATE_CREATED, 
            email AS EMAIL, 
            CAST(employee AS INT) AS EMPLOYEE_ID, 
            ENTITYid AS ENTITY_ID, 
            CAST(ENTITYnumber AS INT) AS ENTITY_NUMBER, 
            ENTITYtitle AS ENTITY_TITLE, 
            firstname AS FIRST_NAME, 
            CAST("group" AS INT) AS GROUP_ID, 
            CAST(id AS INT) AS INTERNAL_ENTITY_ID, 
            isinactive AS IS_INACTIVE, 
            isperson AS IS_PERSON, 
            CAST(lastmodifieddate AS DATE) AS LAST_MODIFIED_DATE, 
            lastname AS LAST_NAME, 
            CAST(parent AS INT) AS PARENT_ID, 
            CAST(partner AS INT) AS PARTNER_ID, 
            "type" AS ENTITY_TYPE, 
            CAST(vendor AS INT) AS VENDOR_ID, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
