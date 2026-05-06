{% snapshot snap_hubspot_deals %}

{{
    config(
        target_schema='snapshots',
        unique_key='deal_id',
        strategy='timestamp',
        updated_at='snapshot_updated_at',
        hard_deletes='invalidate'
    )
}}

with deals as (

    select
        deal_id,
        hs_object_id,

        -- associations
        primary_company_id,
        primary_company_id_from_array,
        company_name,
        company_code,

        -- deal details
        deal_name,
        deal_type,
        pipeline_id,
        deal_stage_id,
        deal_source,

        -- ownership
        hubspot_owner_id,
        hubspot_team_id,

        -- project/client fields
        project_code,
        project_name,
        project_type,
        spoc,
        t_client,

        -- financial fields
        amount,
        amount_in_home_currency,
        closed_amount,
        closed_amount_in_home_currency,
        forecast_amount,
        projected_amount,
        predicted_amount,
        deal_stage_probability,

        -- status flags
        is_archived,
        is_closed,
        is_closed_won,
        is_closed_lost,
        is_stalled,

        -- dates/timestamps
        created_at,
        updated_at,
        hs_last_modified_at,
        close_at,
        close_date,

        -- lifecycle/activity
        days_to_close,
        days_to_close_raw,
        num_notes,
        num_contacted_notes,
        num_associated_contacts,
        notes_last_updated_at,
        notes_last_contacted_at,
        notes_next_activity_at,

        -- attribution
        analytics_source,
        analytics_latest_source,

        -- use the best available update timestamp for snapshot change detection
        coalesce(
            hs_last_modified_at,
            updated_at,
            airbyte_extracted_at,
            current_timestamp()
        ) as snapshot_updated_at

    from {{ ref('stg_hubspot_deals') }}

)

select
    *
from deals

{% endsnapshot %}