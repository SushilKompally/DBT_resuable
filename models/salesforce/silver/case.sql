{{
    config(
        materialized="incremental",
        unique_key="CASE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "case") }}

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
    
    Id               AS CASE_ID,
    AccountId        AS ACCOUNT_ID,
    ContactId        AS CONTACT_ID,
    OwnerId          AS OWNER_USER_ID,
    Status           AS STATUS,
    Priority         AS PRIORITY,
    Origin           AS ORIGIN,
    Reason           AS REASON,
    Subject          AS SUBJECT,
    Description      AS DESCRIPTION,
    CreatedDate      AS CREATED_DATE,
    LastModifiedDate AS LAST_MODIFIED_DATE,
    ClosedDate       AS CLOSED_DATE,
    IsClosed         AS IS_CLOSED,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
