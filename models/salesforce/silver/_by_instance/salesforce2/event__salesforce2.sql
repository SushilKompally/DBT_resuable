{{ config(
    materialized = 'table'
) }}

SELECT *
FROM {{ source('salesforcesalesforce2', 'event') }}
{% if execute %}
    {% if var('start_date') %}
        WHERE {{ var('record_creation_column', 'LastModifiedDate') }} >= '{{ var('start_date') }}'
    {% endif %}
{% endif %}
