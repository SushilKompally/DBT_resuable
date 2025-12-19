{% snapshot dim_campaign %}
{{
    config(
        unique_key="SF_CAMPAIGN_ID",
        strategy="timestamp",
        updated_at="silver_last_updated"
    )
}}

select
    campaign_id as sf_campaign_id,
    name,
    entity_type,
    status,
    start_date,
    end_date,
    owner_user_id,
    silver_last_updated
from {{ ref("campaign") }}

{% endsnapshot %}
