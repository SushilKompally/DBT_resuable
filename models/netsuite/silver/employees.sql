{{
    config(
        materialized="incremental",
        unique_key="EMPLOYEE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "employees") }}

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
            accountnumber AS ACCOUNT_NUMBER, 
            CAST(class_id AS INT) AS CLASS_ID, 
            CAST(create_date AS DATE) AS DATE_CREATED, 
            CAST(def_expense_report_currency_id AS INT) AS CURRENCY, 
            CAST(department_id AS INT) AS DEPATMENT_ID, 
            email AS EMAIL, 
            CAST(employee_id AS INT) AS EMPLOYEE_ID, 
            CAST(employee_type_id AS INT) AS EMPLOYEE_TYPE_ID, 
            ENTITYId AS ENTITY_ID, 
            isinactive AS IS_INACTIVE, 
            job_description AS JOB_DESCRIPTION, 
            CAST(last_modified_date AS DATE) AS LAST_MODIFIED_DATE, 
            CAST(location_id AS INT) AS LOCATION_ID, 
            status AS EMPLOYEE_STATUS_ID, 
            CAST(subsidiary_id AS INT) AS SUBSIDIARY, 
            title AS TITLE, 
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
