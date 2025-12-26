{{ config(
    materialized = 'view'
) }}

WITH consolidated AS (
    SELECT * FROM {{ ref('contact__salesforce1') }}
    UNION ALL
    SELECT * FROM {{ ref('contact__salesforce2') }}
)

SELECT *
FROM consolidated