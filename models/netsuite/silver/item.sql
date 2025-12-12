{{
    config(
        materialized="incremental",
        unique_key="ITEM_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "item") }}

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
            averagecost AS "AVERAGE_COST",
            CAST(class AS INT) AS "CLASS_ID",
            cost AS "COST",
            costingmethod AS "COSTING_METHOD",
            CAST(createddate AS DATE) AS "CREATED_DATE",
            CAST(department AS INT) AS "DEPARTMENT_ID",
            description AS "DESCRIPTION",
            displayname AS "DISPLAY_NAME",
            fullname AS "ITEM_FULL_NAME",
            CAST(id AS INT) AS "ITEM_ID",
            CAST(incomeaccount AS INT) AS "INCOME_ACCOUNT",
            isfulfillable AS "IS_FUL_FILLABLE",
            isinactive AS "IS_INACTIVE",
            ITEMid AS "ITEM_NAME",
            ITEMtype AS "ITEM_TYPE",
            CAST(lastmodifieddate AS DATE) AS "LAST_MODIFIED_DATE",
            lastpurchaseprice AS "LAST_PURCHASE_PRICE",
            CAST(location AS INT) AS "LOCATION_ID",
            manufacturer AS "MANUFACTURER",
            CAST(maximumquantity AS INT) AS "MAXIMUN_QUANTITY",
            CAST(parent AS INT) AS "PARENT",
            CAST(pricinggroup AS INT) AS "PRICING_GROUP",
            quantityonhand AS "TOTAL_QUANTITY_ON_HAND",
            CAST(saleunit AS INT) AS "SALEUNIT",
            shippingcost AS "SHIPPING_COST",
            CAST(stockunit AS INT) AS "STOCK_UNIT",
            storedescription AS "STORE_DESCRIPTION",
            storedetaileddescription AS "STORE_DETAILED_DESCRIPTION",
            storedisplayname AS "STORE_DISPLAY_NAME",
            CAST(SUBSIDIARY AS INT) AS "SUBSIDIARY_ID",
            subtype AS "SUB_TYPE",
            totalvalue AS "TOTAL_VALUE",
            CAST(unitstype AS INT) AS "UNITS_TYPE",
            vendorname AS "VENDOR_NAME",
            weight AS "WEIGHT",
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
