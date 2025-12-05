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
    FROM {{ source("salesforce_bronze", "account") }}

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
            id AS ACCOUNT_ID,
            name AS NAME,
            accountnumber AS ACCOUNT_NUMBER,
            type AS ENTITY_TYPE,
            parentid AS PARENT_ACCOUNT_ID,
            industry AS INDUSTRY,
            annualrevenue AS ANNUAL_REVENUE,
            numberofemployees AS NUMBER_OF_EMPLOYEES,
            rating AS RATING,
            ownership AS OWNERSHIP,
            website AS WEBSITE,
            tickersymbol AS TICKER_SYMBOL,
            phone AS PHONE,
            fax AS FAX,
            billingstreet AS BILLING_STREET,
            billingcity AS BILLING_CITY,
            billingstate AS BILLING_STATE,
            billingpostalcode AS BILLING_POSTAL_CODE,
            billingcountry AS BILLING_COUNTRY,
            shippingstreet AS SHIPPING_STREET,
            shippingcity AS SHIPPING_CITY,
            shippingstate AS SHIPPING_STATE,
            shippingpostalcode AS SHIPPING_POSTAL_CODE,
            shippingcountry AS SHIPPING_COUNTRY,
            site AS SITE,
            ownerid AS OWNER_USER_ID,
            createddate AS CREATED_DATE,
            createdbyid AS CREATED_BY_ID,
            lAStmodifieddate AS LAST_MODIFIED_DATE,
            lAStmodifiedbyid AS LAST_MODIFIED_BY_ID,
            last_updated AS LAST_UPDATED,
            description AS DESCRIPTION,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
