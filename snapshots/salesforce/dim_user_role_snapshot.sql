
{% snapshot dim_user_role_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_USER_ROLE_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


SELECT
    USER_ROLE_ID AS SF_USER_ROLE_ID,
    NAME AS ROLE_NAME,
    USER_ROLE_ID AS PARENT_ROLE_ID,       
    LAST_MODIFIED_DATE,
FROM {{ ref('user_role') }} 
{% endsnapshot %}

