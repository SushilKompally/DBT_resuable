
{{ 
    config(
        materialized="incremental",
        unique_key="CONTACT_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    ) 
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "contact") }}

    {% if is_incremental() %}
        WHERE CAST(LASTMODIFIEDDATE AS TIMESTAMP_NTZ) > (
            SELECT DATEADD(
                DAY, -1,
                COALESCE(MAX(LAST_MODIFIED_DATE)::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ)
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
        Id                  AS CONTACT_ID,
        AccountId           AS ACCOUNT_ID,
        FirstName           AS FIRST_NAME,
        LastName            AS LAST_NAME,
        Salutation          AS SALUTATION,
        Title               AS TITLE,
        Department          AS DEPARTMENT,
        Email               AS EMAIL,
        Phone               AS PHONE,
        MobilePhone         AS MOBILE_PHONE,
        MailingStreet       AS MAILING_STREET,
        MailingCity         AS MAILING_CITY,
        MailingState        AS MAILING_STATE,
        MailingPostalCode   AS MAILING_POSTAL_CODE,
        MailingCountry      AS MAILING_COUNTRY,
        OtherStreet         AS OTHER_STREET,
        OtherCity           AS OTHER_CITY,
        OtherState          AS OTHER_STATE,
        OtherPostalCode     AS OTHER_POSTAL_CODE,
        OtherCountry        AS OTHER_COUNTRY,
        LeadSource          AS LEAD_SOURCE,
        OwnerId             AS OWNER_USER_ID,
        CreatedDate         AS CREATED_DATE,
        CreatedById         AS CREATED_BY_ID,
        LastModifiedDate    AS LAST_MODIFIED_DATE,
        LastModifiedById    AS LAST_MODIFIED_BY_ID,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS SILVER_LOAD_DATE
    FROM RAW 
)

SELECT *
FROM CLEANED
