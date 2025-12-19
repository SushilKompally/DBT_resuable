
{{ config(
    database='salesforce_db',
    schema='test_gold',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sf_opportunity_id',
) }}

with opportunity_base as (
    select
        {{ dbt_utils.surrogate_key(['so.opportunity_id']) }}      as opportunity_key,
        so.opportunity_id                                         as sf_opportunity_id,
        so.account_id,
        so.owner_user_id,
        so.stage_name,
        so.amount                                                 as amount,
        cast(so.close_date as date)                               as close_date,         
        so.last_modified_date                                     as last_modified_date  
    from {{ ref('opportunity') }} so
    {{ incremental_where(
        source_ts_col='so.last_modified_date',
        target_ts_col='last_modified_date',
        lookback_days=env_var('DBT_LOOKBACK_DAYS', '1')
    ) }}
),

dim_joins as (
    select
        ob.opportunity_key,
        ob.sf_opportunity_id,
        du.dbt_scd_id                                             as owner_user_key,
        da.dbt_scd_id                                             as account_key,
        dos.opportunity_stage_key                                 as stage_key,
        ob.amount,
        to_number(to_varchar(ob.close_date, 'YYYYMMDD'))          as close_date_key,
        ob.last_modified_date
    from opportunity_base ob
    left join {{ ref('dim_user_snapshot') }} du
      on ob.owner_user_id = du.sf_user_id
     and du.dbt_valid_to is null
    left join {{ ref('dim_account_snapshot') }} da
      on ob.account_id = da.sf_account_id
     and da.dbt_valid_to is null
    left join {{ ref('dim_opportunity_stage') }} dos
      on ob.stage_name = dos.stage_name
),

final as (
    select
        opportunity_key,
        sf_opportunity_id,
        account_key,
        owner_user_key,
        stage_key,
        amount,
        close_date_key,
        last_modified_date
    from dim_joins
)

select *
from final
