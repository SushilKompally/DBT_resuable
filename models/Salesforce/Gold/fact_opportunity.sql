
{{ config(
    database='SALESFORCE_DB',
    schema='TEST_GOLD',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SF_OPPORTUNITY_ID',
    on_schema_change='sync_all_columns'
) }}


SELECT
    {{ dbt_utils.surrogate_key(['so.opportunity_id']) }} AS OPPORTUNITY_KEY,
    so.opportunity_id         AS SF_OPPORTUNITY_ID,
    da.DBT_SCD_ID AS  ACCOUNT_KEY,
    du.DBT_SCD_ID AS OWNER_USER_KEY,
    dos.OPPORTUNITY_STAGE_KEY AS STAGE_KEY,
    so.AMOUNT                 AS AMOUNT,
    dd.date_key               AS CLOSE_DATE_KEY,
    so.LAST_MODIFIED_DATE
FROM {{ ref('opportunity') }} so
LEFT JOIN {{ ref('dim_account_snapshot') }} da 
       ON so.account_id = da.sf_account_id 
      AND da.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_user_snapshot') }} du 
       ON so.owner_user_id = du.sf_user_id 
      AND du.dbt_valid_to IS NULL
LEFT JOIN {{ ref('dim_opportunity_stage') }} dos
       ON so.stage_name = dos.stage_name 
LEFT JOIN {{ ref('dim_dates') }} dd 
       ON TO_NUMBER(TO_VARCHAR(CAST(so.close_date AS DATE), 'YYYYMMDD')) = dd.date_key

{% if is_incremental() %}
    WHERE so.LAST_MODIFIED_DATE > (SELECT MAX(LAST_MODIFIED_DATE) FROM {{ this }})  -- Filtering only new or updated records
{% endif %}
