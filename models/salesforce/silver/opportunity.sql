{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('opportunity__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('opportunity__salesforce2') }}
)

SELECT *
FROM consolidated