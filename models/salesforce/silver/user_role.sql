{{
    config(
        materialized="incremental",
        unique_key="USER_ROLE_ID",
        incremental_strategy="merge",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "userrole") }}

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
    Id               AS USER_ROLE_ID,
    Name             AS NAME,
    DeveloperName    AS DEVELOPER_NAME,
    ParentRoleId     AS PARENT_ROLE_ID,
    RollupDescription AS ROLLUP_DESCRIPTION,
    BusinessHoursId  AS BUSINESS_HOURS_ID,
    ForecastUserId   AS FORECAST_USER_ID,
    CreatedDate      AS CREATED_DATE,
    LastModifiedDate AS LAST_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED

WITH consolidated AS (
    SELECT * FROM { ref('user_role__salesforce1') }
    UNION ALL
    SELECT * FROM { ref('user_role__salesforce2') }
)

SELECT *
FROM consolidated