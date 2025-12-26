{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('user__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('user__salesforce2') }}
)

SELECT *
FROM consolidated