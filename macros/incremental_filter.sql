
 
{% macro incremental_filter(source_ts_col, target_ts_col, lookback_days=1, target_relation=None) %}

{% if is_incremental() %}
  AND {{ source_ts_col }}::timestamp_ntz >
    (
      SELECT dateadd(
        day,
        -{{ lookback_days }},
        coalesce(max({{ target_ts_col }}), '1900-01-01'::timestamp_ntz)
      )
      FROM {{ target_relation if target_relation is not none else this }}
    )
{% endif %}
{% endmacro %}
