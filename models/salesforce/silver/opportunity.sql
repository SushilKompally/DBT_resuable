{{
    config(
        materialized="incremental",
        unique_key="OPPORTUNITY_ID",
        incremental_strategy="merge",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "opportunity") }}

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
    Id                        AS OPPORTUNITY_ID,
    AccountId                 AS ACCOUNT_ID,
    OwnerId                   AS OWNER_USER_ID,
    Name                      AS NAME,
    StageName                 AS STAGE_NAME,
    Amount                    AS AMOUNT,
    CloseDate                 AS CLOSE_DATE,
    Probability               AS PROBABILITY,
    Type                    AS ENTITY_TYPE,
    LeadSource                AS LEAD_SOURCE,
    CampaignId                AS CAMPAIGN_ID,
    ForecastCategoryName      AS FORECAST_CATEGORY_NAME,
    IsClosed                  AS IS_CLOSED,
    IsWon                     AS IS_WON,
    NextStep                  AS NEXT_STEP,
    PrimaryCompetitor         AS PRIMARY_COMPETITOR,
    CreatedDate               AS CREATED_DATE,
    LastModifiedDate          AS LAST_MODIFIED_DATE,
    Description               AS DESCRIPTION,
    CURRENT_TIMESTAMP()::TIMESTAMP AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
