{{ config(
    unique_key='account_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with raw as (

    select
        *,
        {{ source_data(
              tool_name='snowflake',
              record_creation_column='lastmodifieddate'
          ) }}
    from {{ source('salesforce_bronze', 'account') }}

    {{ incremental_where(
        source_ts_col='lastmodifieddate',
        target_ts_col='last_modified_date',
        lookback_days=1
    ) }}
),

cleaned as (
    
select
    -- PRIMARY KEY
    id                 as account_id,

    -- FOREIGN KEYS
    parentid           as parent_account_id,
    ownerid            as owner_user_id,

    -- NUMERIC
    annualrevenue      as annual_revenue,
    numberofemployees  as number_of_employees,

    -- DETAILS
    name               as name,
    accountnumber      as account_number,
    type               as entity_type,
    industry           as industry,
    rating             as rating,
    ownership          as ownership,
    website            as website,
    tickersymbol       as ticker_symbol,
    phone              as phone,
    fax                as fax,
    billingstreet      as billing_street,
    billingcity        as billing_city,
    billingstate       as billing_state,
    billingpostalcode  as billing_postal_code,
    billingcountry     as billing_country,
    shippingstreet     as shipping_street,
    shippingcity       as shipping_city,
    shippingstate      as shipping_state,
    shippingpostalcode as shipping_postal_code,
    shippingcountry    as shipping_country,
    site               as site,
    description        as description,

    -- TIMESTAMPS 
    createddate        as created_date,
    cast(lastmodifieddate as timestamp_ntz)   as last_modified_date,

    -- CREATED BY 
    createdbyid        as created_by_id
from raw

)

select *
from cleaned
