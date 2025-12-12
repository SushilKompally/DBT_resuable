
{% snapshot dim_chartofaccounts %}
{{
  config(
    target_database='NETSUITETEST_DB',
    target_schema='TEST_GOLD',
    unique_key='ACCOUNT_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


   SELECT
    a.ACCOUNT_ID,
    a.ACCOUNT_NUMBER,
    a.CLASS_ID,
    COALESCE(a.CURRENCY_ID, s.CURRENCY_ID) AS CURRENCY_ID,
    a.DEPARTMENT_ID,
    a.ACCOUNT_DESCRIPTION,
    a.DISPLAY_NAME,
    a.DISPLAY_NAME_WITH_HIERARCHY,
    a.LOCATION_ID,
    a.ACCOUNT_NAME,
    a.PARENT_ID AS ACCOUNT_PARENT_ID,
    COALESCE(a.SUBSIDIARY_ID, s.SUBSIDIARY_ID) AS SUBSIDIARY_ID,
    s.SUBSIDIARY_NAME,
    s.SUBSIDIARY_FULL_NAME,
    s.PARENT_ID AS SUBSIDIARY_PARENT_ID,
    a.ACCOUNT_TYPE,
    c.CLASS_FULL_NAME,
    c.CLASS_NAME,
    c.PARENT AS CLASSIFICATION_PARENT,
    TRUE AS IS_CURRENT,
    CURRENT_TIMESTAMP() AS VALID_FROM,
    NULL AS VALID_TO
  FROM {{ ref('Accounts') }} a
  LEFT JOIN {{ ref('classification') }} c ON a.CLASS_ID = c.CLASS_ID
  LEFT JOIN {{ ref('subsidiaries') }} ON a.SUBSIDIARY_ID = s.SUBSIDIARY_ID
{% endsnapshot %}

