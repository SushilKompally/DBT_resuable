{{ config(
    materialized = 'ephemeral',
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns'
) }}

WITH raw AS (
    SELECT
        *,
        {{ source_metadata() }}
    FROM {{ source('salesforcesalesforce2', 'contact') }}
    WHERE 1=1
    {{ incremental_filter() }}
),

cleaned AS (
    SELECT * FROM raw
)

SELECT * FROM cleaned