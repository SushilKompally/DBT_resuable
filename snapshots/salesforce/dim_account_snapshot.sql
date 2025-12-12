
{% snapshot dim_account_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_ACCOUNT_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


  SELECT
    ACCOUNT_ID AS SF_ACCOUNT_ID,
    NAME,
    ACCOUNT_NUMBER,
    ENTITY_TYPE,
    INDUSTRY,
    ANNUAL_REVENUE,
    NUMBER_OF_EMPLOYEES,
    OWNER_USER_ID,
    BILLING_CITY,
    SHIPPING_CITY,
    CREATED_DATE,
    LAST_MODIFIED_DATE
FROM {{ ref('account') }}   
{% endsnapshot %}

