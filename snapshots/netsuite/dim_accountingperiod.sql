
{% snapshot dim_accounting_period %}
{{
  config(
    target_database='NETSUITETEST_DB',
    target_schema='TEST_GOLD',
    unique_key='POSTING_PERIOD_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


  SELECT
   POSTING_PERIOD_ID, 
    CLOSED_ON_DATE,
    LAST_MODIFIED_DATE,
    END_DATE, 
    START_DATE,
    PERIOD_NAME, 
    YEAR,
    LAST_MODIFIED_DATE
FROM {{ ref('Accounting_periods') }}   -- source from your Silver model
{% endsnapshot %}

