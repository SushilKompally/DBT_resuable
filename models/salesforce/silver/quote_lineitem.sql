{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('quote_lineitem__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('quote_lineitem__salesforce2') }}
)

SELECT *
FROM consolidated