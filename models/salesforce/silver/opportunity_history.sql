{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('opportunity_history__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('opportunity_history__salesforce2') }}
)

SELECT *
FROM consolidated