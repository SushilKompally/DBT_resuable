
{% macro source_metadata(tool_name="fivetran", record_creation_column="lastmodifieddate") -%}

    {% if tool_name == "fivetran" -%}
         LastModifiedDate AS _source_timestamp,
         COALESCE(TRY_CAST(ISDELETED AS BOOLEAN), FALSE) AS is_deleted


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
        
    {%- endif %}

{%- endmacro %}
