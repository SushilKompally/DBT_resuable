{{
    config(
        materialized="incremental",
        unique_key="QUOTE_ID",
        incremental_strategy="merge",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "quote") }}

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
    Id              AS QUOTE_ID,
    OpportunityId   AS OPPORTUNITY_ID,
    AccountId       AS ACCOUNT_ID,
    Status          AS STATUS,
    QuoteNumber     AS QUOTE_NUMBER,
    Name            AS NAME,
    ExpirationDate  AS EXPIRATION_DATE,
    BillingStreet   AS BILLING_STREET,
    BillingCity     AS BILLING_CITY,
    BillingState    AS BILLING_STATE,
    BillingPostalCode AS BILLING_POSTAL_CODE,
    BillingCountry  AS BILLING_COUNTRY,
    ShippingStreet  AS SHIPPING_STREET,
    ShippingCity    AS SHIPPING_CITY,
    ShippingState   AS SHIPPING_STATE,
    ShippingPostalCode AS SHIPPING_POSTAL_CODE,
    ShippingCountry AS SHIPPING_COUNTRY,
    TotalAmount     AS TOTAL_AMOUNT,
    Subtotal        AS SUBTOTAL,
    Discount        AS DISCOUNT,
    GrandTotal      AS GRAND_TOTAL,
    CreatedDate     AS CREATED_DATE,
    LastModifiedDate AS LAST_MODIFIED_DATE,
    OwnerId         AS OWNER_USER_ID,
    Pricebook2Id    AS PRICEBOOK2_ID,
    CURRENT_TIMESTAMP()::TIMESTAMP AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
