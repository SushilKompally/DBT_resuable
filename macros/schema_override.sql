{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set dbt_env = env_var("DBT_ENVIRONMENT_NAME", "Unknown") -%}
    {%- set deploy_shared = var("deploy_to_shared_silver", false) -%}
 
    {%- if dbt_env in ["Production", "Staging"] -%}
        {{ custom_schema_name | trim }}
 
    {%- elif dbt_env == "Development" and deploy_shared -%}
        {{ custom_schema_name | trim }}
 
    {%- elif dbt_env == "Development" -%}
        {{ default_schema | trim }}_{{ custom_schema_name | trim }}
 
    {%- else -%}
        {{ default_schema | trim }}
    {%- endif -%}
{%- endmacro %}