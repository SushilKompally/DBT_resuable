{{
    config(
        materialized="incremental",
        unique_key="USER_ID",
        incremental_strategy="merge",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "user") }}

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
    Id              AS USER_ID,
    Username        AS USERNAME,
    Email           AS EMAIL,
    Alias           AS ALIAS,
    FirstName       AS FIRST_NAME,
    LastName        AS LAST_NAME,
    IsActive        AS IS_ACTIVE,
    UserRoleId      AS USER_ROLE_ID,
    ProfileId       AS PROFILE_ID,
    Title           AS TITLE,
    Department      AS DEPARTMENT,
    ManagerId       AS MANAGER_ID,
    CreatedDate     AS CREATED_DATE,
    LastLoginDate   AS LAST_LOGIN_DATE,
    LastModifiedDate AS LAST_MODIFIED_DATE,
    TimeZoneSidKey  AS TIME_ZONE_SID_KEY,
    LocaleSidKey    AS LOCALE_SID_KEY,
    LanguageLocaleKey AS LANGUAGE_LOCALE_KEY,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED

WITH consolidated AS (
    SELECT * FROM { ref('user__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('user__salesforce2') }
)

SELECT *
FROM consolidated