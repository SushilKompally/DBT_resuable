{% snapshot dim_userrole %}
{{
    config(
        unique_key='USER_ROLE_ID',
        strategy='timestamp',
        updated_at='silver_last_updated'
    )
}}

SELECT
    USER_ROLE_ID,
    NAME AS ROLE_NAME,
    USER_ROLE_ID AS PARENT_ROLE_KEY,
    -- CREATED_DATE,
    LAST_MODIFIED_DATE,
    --silver_last_updated
FROM {{ ref('user_role') }}
{% endsnapshot %}
