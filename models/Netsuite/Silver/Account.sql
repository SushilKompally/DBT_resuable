{{ config(
    materialized='incremental',
    unique_key='ACCOUNT_ID',
    incremental_strategy = 'merge',
    on_schema_change='sync_all_columns'
) }}

with raw as
(
    select * from {{ source("salesforce_bronze", "account") }}
        
        {% if is_incremental() %}
        where
        cast(LASTMODIFIEDDATE as timestamp_ntz) > (
            select dateadd(day, -1, coalesce(max(last_modified_date), '1900-01-01'::timestamp_ntz))
            from {{ this }}
        )
        and 1=1
        {% else %}
        where 1=1
        {% endif %}
),

cleaned as (
select
    id as account_id,
    name as name,
    accountnumber as account_number,
    type as entity_type,
    parentid as parent_account_id,
    industry as industry,
    annualrevenue as annual_revenue,
    numberofemployees as number_of_employees,
    rating as rating,
    ownership as ownership,
    website as website,
    tickersymbol as ticker_symbol,
    phone as phone,
    fax as fax,
    billingstreet as billing_street,
    billingcity as billing_city,
    billingstate as billing_state,
    billingpostalcode as billing_postal_code,
    billingcountry as billing_country,
    shippingstreet as shipping_street,
    shippingcity as shipping_city,
    shippingstate as shipping_state,
    shippingpostalcode as shipping_postal_code,
    shippingcountry as shipping_country,
    site as site,
    ownerid as owner_user_id,
    createddate as created_date,
    createdbyid as created_by_id,
    lastmodifieddate as last_modified_date,
    lastmodifiedbyid as last_modified_by_id,
    last_updated,
    description as description,
    current_timestamp() as silver_load_date
 from raw
 )

 select * from cleaned
