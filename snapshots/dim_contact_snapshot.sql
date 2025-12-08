
{% snapshot dim_contact_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_CONTACT_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


  SELECT
    c.CONTACT_ID AS SF_CONTACT_ID,
    da.ACCOUNT_ID ,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL,
    c.PHONE,
    c.CREATED_DATE,
    c.LAST_MODIFIED_DATE
FROM {{ ref('contact') }} c
LEFT JOIN {{ ref('Account') }} da
    ON c.ACCOUNT_ID = da.ACCOUNT_ID   
{% endsnapshot %}

