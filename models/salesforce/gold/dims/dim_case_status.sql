{{
    config(
        materialized="table",
        on_schema_change="sync_all_columns",
    )
}}

WITH distinct_cases AS (
    SELECT DISTINCT
        CASE_ID,
        STATUS,
        IS_CLOSED
    FROM {{ ref('case') }}
)

SELECT 
    {{ dbt_utils.surrogate_key(['STATUS', 'IS_CLOSED']) }} AS CASE_STATUS_KEY,
    STATUS AS STATUS_NAME,
    IS_CLOSED
FROM distinct_cases