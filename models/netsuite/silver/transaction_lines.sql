{{
    config(
        materialized="incremental",
        unique_key="TRANSACTION_LINE_ID",
        incremental_strategy="merge",
        on_schema_change="sync_all_columns",
    )
}}

WITH RAW AS (

    SELECT *
    FROM {{ source("netsuite_bronze", "transaction_lines") }}

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
            accountinglinetype AS ACCOUNTING_LINE_TYPE,
            CAST(actualshipdate AS DATE) AS ACTUAL_SHIP_DATE,
            CAST(billeddate AS DATE) AS BILLED_DATE,
            CAST(billingschedule AS INT) AS BILLING_SCHEDULE_ID,
            CAST(class AS INT) AS CLASS_ID,
            CAST(closedate AS DATE) AS CLOSE_DATE,
            CAST(createdfrom AS INT) AS CREATED_FROM_TRANSACTION_ID,
            CAST(department AS INT) AS DEPARTMENT_ID,
            CAST(ENTITY AS INT) AS ENTITY_ID,
            CAST(expenseaccount AS INT) AS REVENUE_ACCOUNT_NAME,
            foreignamount AS FOREIGN_AMOUNT,
            CAST(id AS INT) AS TRANSACTION_LINE_ID,
            isbillable AS IS_BILLABLE,
            isclosed AS IS_CLOSED,
            iscogs AS IS_COGS,
            isfullyshipped AS IS_FULLY_SHIPPED,
            CAST(ITEM AS INT) AS ITEM_ID,
            ITEMtype AS ITEM_TYPE,
            CAST(linelastmodifieddate AS DATE) AS LINE_LASTMODIFIED_DATE,
            CAST(linesequencenumber AS INT) AS LINE_SEQUENCE_NUMBER_ID,
            CAST(location AS INT) AS LOCATION_ID,
            memo AS MEMO,
            netamount AS NET_AMOUNT,
            orderpriority AS ORDER_PRIORITY,
            CAST(paymentmethod AS INT) AS PAYMENT_METHOD_ID,
            CAST(price AS INT) AS PRICE_ID,
            quantity AS QUANTITY,
            rate AS RATE,
            CAST(revenueelement AS INT) AS ZAB_REVENUE_DETAIL_ID,
            CAST(SUBSIDIARY AS INT) AS SUBSIDIARY_ID,
            taxline AS TAX_LINE,
            CAST(transaction AS INT) AS TRANSACTION_ID,
            transactiondiscount AS TRANSACTION_DISCOUNT,
            transactionlinetype AS TRANSACTION_LINE_TYPE,
            CAST(uniquekey AS INT) AS UNIQUE_KEY,
            CAST(units AS INT) AS UNIT_ID,
            current_timestamp() AS SILVER_LOAD_DATE
          FROM RAW
    )

SELECT *
FROM CLEANED
