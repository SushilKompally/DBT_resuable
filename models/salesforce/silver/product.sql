{{
    config(
        materialized="incremental",
        unique_key="PRODUCT_ID",
        incremental_strategy="merge"
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "product") }}

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
    Id                   AS PRODUCT_ID,
    Name                 AS NAME,
    ProductCode          AS PRODUCT_CODE,
    Description          AS DESCRIPTION,
    Family               AS FAMILY,
    IsActive             AS IS_ACTIVE,
    CreatedDate          AS CREATED_DATE,
    LastModifiedDate     AS LAST_MODIFIED_DATE,
    QuantityUnitOfMeasure AS QUANTITY_UNIT_OF_MEASURE,
    VendorProductCode    AS VENDOR_PRODUCT_CODE,
    Manufacturer         AS MANUFACTURER,
    CURRENT_TIMESTAMP()  AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
