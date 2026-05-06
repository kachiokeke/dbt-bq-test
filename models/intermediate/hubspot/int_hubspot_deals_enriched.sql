with deals as (

    select
        *
    from {{ ref('stg_hubspot_deals') }}

),

enriched as (

    select
        -- primary identifiers
        deal_id,
        hs_object_id,
        primary_company_id,
        primary_company_id_from_array,

        -- deal details
        deal_name,
        deal_type,
        pipeline_id,
        deal_stage_id,
        deal_source,
        company_name,
        company_code,

        -- ownership
        hubspot_owner_id,
        hubspot_team_id,
        all_owner_ids,
        all_team_ids,

        -- project/client details
        project_code,
        project_name,
        project_type,
        spoc,
        t_client,

        -- timestamps
        created_at,
        updated_at,
        close_at,
        close_date,

        date(created_at) as created_date,
        date(updated_at) as updated_date,

        date_trunc(date(created_at), month) as created_month,
        date_trunc(date(close_at), month) as close_month,

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

        -- derived deal status
        case
            when is_closed_won = true then 'Closed Won'
            when is_closed_lost = true then 'Closed Lost'
            when is_closed = true then 'Closed Other'
            else 'Open'
        end as deal_status,

        case
            when is_closed = true then 'Closed'
            else 'Open'
        end as open_closed_status,

        -- amount logic
        coalesce(amount, amount_in_home_currency, closed_amount, closed_amount_in_home_currency, 0) as effective_amount,

        coalesce(amount, 0) * coalesce(deal_stage_probability, 0) as weighted_pipeline_amount,

        case
            when amount is null then 'No Amount'
            when amount < 5000 then 'Under 5k'
            when amount < 20000 then '5k - 20k'
            when amount < 50000 then '20k - 50k'
            else '50k+'
        end as amount_bucket,

        -- useful analysis flags
        case
            when amount is not null then true
            else false
        end as has_amount,

        case
            when company_name is not null then true
            else false
        end as has_company,

        case
            when primary_company_id is not null
              or primary_company_id_from_array is not null
            then true
            else false
        end as has_company_association,

        case
            when close_at is not null then true
            else false
        end as has_close_date,

        -- lifecycle fields
        days_to_close,
        days_to_close_raw,

        case
            when created_at is not null
            then date_diff(current_date(), date(created_at), day)
        end as deal_age_days,

        case
            when close_at is not null and created_at is not null
            then date_diff(date(close_at), date(created_at), day)
        end as created_to_close_days,

        -- activity fields
        num_notes,
        num_contacted_notes,
        num_associated_contacts,
        notes_last_updated_at,
        notes_last_contacted_at,
        notes_next_activity_at,

        case
            when notes_last_contacted_at is not null
            then date_diff(current_date(), date(notes_last_contacted_at), day)
        end as days_since_last_contact,

        -- attribution fields
        analytics_source,
        analytics_source_data_1,
        analytics_source_data_2,
        analytics_latest_source,
        analytics_latest_source_data_1,
        analytics_latest_source_data_2,

        -- metadata
        airbyte_extracted_at

    from deals

)

select
    *
from enriched