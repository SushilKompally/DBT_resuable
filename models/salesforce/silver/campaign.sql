{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('campaign__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('campaign__salesforce2') }}
)

SELECT *
FROM consolidated