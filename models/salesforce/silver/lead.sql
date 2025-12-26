{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('lead__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('lead__salesforce2') }}
)

SELECT *
FROM consolidated