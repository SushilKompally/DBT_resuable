{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('account__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('account__salesforce2') }}
)

SELECT *
FROM consolidated