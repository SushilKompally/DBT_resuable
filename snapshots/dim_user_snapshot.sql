
{% snapshot dim_user_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_USER_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


 SELECT
    u.USER_ID AS SF_USER_ID,
    u.USERNAME,
    u.EMAIL,
    u.FIRST_NAME,
    u.LAST_NAME,
    u.IS_ACTIVE,
    r.USER_ROLE_ID,  
    u.CREATED_DATE,
    u.last_modified_date  
FROM {{ ref('user') }} u
LEFT JOIN {{ ref('user_role') }} r 
    ON u.USER_ROLE_ID = r.USER_ROLE_ID
{% endsnapshot %}

