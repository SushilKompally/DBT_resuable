{{ config(
    materialized = 'table'
) }}

SELECT *
FROM {{ source('salesforce__INSTANCE__', 'event') }}
{% if execute %}
    {% if var('start_date') %}
        WHERE {{ var('record_creation_column', 'LastModifiedDate') }} >= '{{ var('start_date') }}'
    {% endif %}
{% endif %}
