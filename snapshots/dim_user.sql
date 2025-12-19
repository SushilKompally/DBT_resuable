{% snapshot dim_user %}
    {{
        config(
            unique_key="SF_USER_ID",
            strategy="timestamp",
            updated_at="silver_last_updated",
        )
    }}

    select
        u.user_id as sf_user_id,
        u.username,
        u.email,
        u.first_name,
        u.last_name,
        u.is_active,
        r.user_role_id as user_role_key,  -- Surrogate key from the user role table
        u.created_date,
    from {{ ref("user") }} u
    left join
        {{ ref("user_role") }} r  -- Join to the user role table to get the user role key
        on u.user_role_id = r.user_role_id
{% endsnapshot %}
