{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('product__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('product__salesforce2') }}
)

SELECT *
FROM consolidated