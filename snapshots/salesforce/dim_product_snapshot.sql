
{% snapshot dim_product_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_PRODUCT_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


  SELECT
    PRODUCT_ID AS SF_PRODUCT_ID,
    NAME ,
    PRODUCT_CODE,
    FAMILY,
    IS_ACTIVE,
    LAST_MODIFIED_DATE
FROM {{ ref('product') }} 
{% endsnapshot %}

