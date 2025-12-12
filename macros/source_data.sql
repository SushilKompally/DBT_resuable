
{% macro source_data(tool_name="datastream", record_creation_column="inserted_at") -%}

    {% if tool_name == "fivetran" -%}
        _fivetran_synced as _source_timestamp, _fivetran_deleted as is_deleted

    {%- elif tool_name == "stitch" -%}
        _sdc_received_at as _source_timestamp,
        case
            when _sdc_deleted_at is null
            then cast(false as bool)
            else cast(true as bool)
        end as is_deleted

    {%- elif tool_name == "stitch_legacy" -%}
        _sdc_received_at as _source_timestamp,
        false as is_deleted

    {%- elif tool_name == "datastream" -%}
        timestamp_millis(datastream_metadata.source_timestamp) as _source_timestamp,
        cast(false as bool) as is_deleted

    {%- elif tool_name == "datastream_append_mode" -%}
        timestamp_millis(datastream_metadata.source_timestamp) as _source_timestamp,
        datastream_metadata.change_sequence_number as change_sequence_number,
        (
            timestamp_diff(timestamp_millis(datastream_metadata.source_timestamp), {{ record_creation_column }}, day) <= 90
            and datastream_metadata.change_type = 'DELETE'
        ) as is_deleted

    {%- elif tool_name == "snowflake" -%}
        cast({{ record_creation_column }} as timestamp_ntz) as _source_timestamp,
        
    {%- endif %}

{%- endmacro %}
