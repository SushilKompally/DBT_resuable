{{
    config(
        materialized="incremental",
        unique_key="QUOTE_LINE_ITEM_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("salesforce_bronze", "quote_lineitem") }}

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
    Id               AS QUOTE_LINE_ITEM_ID,
    QuoteId          AS QUOTE_ID,
    Product2Id       AS PRODUCT_ID,
    Quantity         AS QUANTITY,
    UnitPrice        AS UNIT_PRICE,
    ServiceDate      AS SERVICE_DATE,
    Discount         AS DISCOUNT,
    TotalPrice       AS TOTAL_PRICE,
    CreatedDate      AS CREATED_DATE,
    LastModifiedDate AS LAST_MODIFIED_DATE,
    CURRENT_TIMESTAMP() AS SILVER_LOAD_DATE
      FROM RAW
    )

SELECT *
FROM CLEANED
