{{
    config(
        materialized="incremental",
        unique_key="CAMPAIGN_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "Campaign") }}

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
    Id                AS CAMPAIGN_ID,
    Name              AS NAME,
    TYPE              AS ENTITY_TYPE,
    Status            AS STATUS,
    StartDate         AS START_DATE,
    EndDate           AS END_DATE,
    ExpectedRevenue   AS EXPECTED_REVENUE,
    BudgetedCost      AS BUDGETED_COST,
    ActualCost        AS ACTUAL_COST,
    NumberSent        AS NUMBER_SENT,
    OwnerId           AS OWNER_USER_ID,
    Description       AS DESCRIPTION,
    CreatedDate       AS CREATED_DATE,
    LastModifiedDate  AS LAST_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
