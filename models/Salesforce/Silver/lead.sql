{{
    config(
        materialized="incremental",
        unique_key="LEAD_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "lead") }}

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
    Id                    AS LEAD_ID,
    OwnerId               AS OWNER_USER_ID,
    Company               AS COMPANY,
    FirstName             AS FIRST_NAME,
    LastName              AS LAST_NAME,
    Salutation            AS SALUTATION,
    Title                 AS TITLE,
    Email                 AS EMAIL,
    Phone                 AS PHONE,
    MobilePhone           AS MOBILE_PHONE,
    Website               AS WEBSITE,
    LeadSource            AS LEAD_SOURCE,
    Status                AS STATUS,
    Rating                AS RATING,
    Industry              AS INDUSTRY,
    AnnualRevenue         AS ANNUAL_REVENUE,
    NumberOfEmployees     AS NUMBER_OF_EMPLOYEES,
    Street                AS STREET,
    City                  AS CITY,
    State                 AS STATE,
    PostalCode            AS POSTAL_CODE,
    Country               AS COUNTRY,
    ConvertedDate         AS CONVERTED_DATE,
    ConvertedAccountId    AS CONVERTED_ACCOUNT_ID,
    ConvertedContactId    AS CONVERTED_CONTACT_ID,
    ConvertedOpportunityId AS CONVERTED_OPPORTUNITY_ID,
    IsConverted           AS IS_CONVERTED,
    CreatedDate           AS CREATED_DATE,
    CreatedById           AS CREATED_BY_ID,
    LastModifiedDate      AS LAST_MODIFIED_DATE,
    LastModifiedById      AS LAST_MODIFIED_BY_ID,
    Description           AS DESCRIPTION,
    CURRENT_TIMESTAMP()   AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
