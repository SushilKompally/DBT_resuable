
{% snapshot dim_campaign_snapshot %}
{{
  config(
    target_database='SALESFORCE_DB',
    target_schema='TEST_GOLD',
    unique_key='SF_CAMPAIGN_ID',
    strategy='timestamp',
    updated_at='last_modified_date',
    invalidate_hard_deletes=true
  )
}}


  SELECT
    CAMPAIGN_ID AS SF_CAMPAIGN_ID,
    NAME,
    ENTITY_TYPE,
    STATUS,
    START_DATE,
    END_DATE,
    OWNER_USER_ID,
    LAST_MODIFIED_DATE
 FROM {{ ref('campaign') }}   -- source from your Silver model
{% endsnapshot %}

