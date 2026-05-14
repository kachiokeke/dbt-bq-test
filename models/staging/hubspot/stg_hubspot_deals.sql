with source as (

    select
        *
    from {{ source('hubspot_raw', 'deals') }}

),

renamed as (

    select
        -- airbyte metadata
        cast(_airbyte_raw_id as string) as airbyte_raw_id,
        safe_cast(_airbyte_extracted_at as timestamp) as airbyte_extracted_at,
        safe_cast(_airbyte_generation_id as int64) as airbyte_generation_id,

        -- primary identifiers
        cast(id as string) as deal_id,
        cast(properties_hs_object_id as string) as hs_object_id,

        -- associations
        contacts as contact_ids_json,
        companies as company_ids_json,
        safe_cast(json_value(companies, '$[0]') as string) as primary_company_id_from_array,
        cast(properties_hs_primary_associated_company as string) as primary_company_id,

        -- timestamps
        safe_cast(createdAt as timestamp) as created_at,
        safe_cast(updatedAt as timestamp) as updated_at,
        safe_cast(properties_createdate as timestamp) as property_created_at,
        safe_cast(properties_hs_createdate as timestamp) as hs_created_at,
        safe_cast(properties_hs_lastmodifieddate as timestamp) as hs_last_modified_at,
        safe_cast(properties_closedate as timestamp) as close_at,
        date(safe_cast(properties_closedate as timestamp)) as close_date,

        -- deal fields
        cast(properties_dealname as string) as deal_name,
        cast(properties_dealtype as string) as deal_type,
        cast(properties_pipeline as string) as pipeline_id,
        cast(properties_dealstage as string) as deal_stage_id,
        cast(properties_deal_source as string) as deal_source,
        cast(properties_company_name as string) as company_name,
        cast(properties_company_code__sync_ as string) as company_code,

        -- owner/team fields
        cast(properties_hubspot_owner_id as string) as hubspot_owner_id,
        cast(properties_hubspot_team_id as string) as hubspot_team_id,
        cast(properties_hs_all_owner_ids as string) as all_owner_ids,
        cast(properties_hs_all_team_ids as string) as all_team_ids,

        -- project/client fields
        cast(properties_project_code as string) as project_code,
        cast(properties_project_name as string) as project_name,
        cast(properties_project_type as string) as project_type,
        cast(properties_spoc as string) as spoc,
        cast(properties_t_client as string) as t_client,

        -- financial fields
        safe_cast(properties_amount as numeric) as amount,
        safe_cast(properties_amount_in_home_currency as numeric) as amount_in_home_currency,
        safe_cast(properties_hs_closed_amount as numeric) as closed_amount,
        safe_cast(properties_hs_closed_amount_in_home_currency as numeric) as closed_amount_in_home_currency,
        safe_cast(properties_hs_forecast_amount as numeric) as forecast_amount,
        safe_cast(properties_hs_projected_amount as numeric) as projected_amount,
        safe_cast(properties_hs_predicted_amount as numeric) as predicted_amount,
        safe_cast(properties_hs_deal_stage_probability as numeric) as deal_stage_probability,

        -- status flags
        safe_cast(archived as bool) as is_archived,
        safe_cast(properties_hs_is_closed as bool) as is_closed,
        safe_cast(properties_hs_is_closed_won as bool) as is_closed_won,
        safe_cast(properties_hs_is_closed_lost as bool) as is_closed_lost,
        safe_cast(properties_hs_is_stalled as bool) as is_stalled,

        -- lifecycle fields
        safe_cast(properties_days_to_close as int64) as days_to_close,
        safe_cast(properties_hs_days_to_close_raw as numeric) as days_to_close_raw,

        -- activity fields
        safe_cast(properties_num_notes as int64) as num_notes,
        safe_cast(properties_num_contacted_notes as int64) as num_contacted_notes,
        safe_cast(properties_num_associated_contacts as int64) as num_associated_contacts,
        safe_cast(properties_notes_last_updated as timestamp) as notes_last_updated_at,
        safe_cast(properties_notes_last_contacted as timestamp) as notes_last_contacted_at,
        safe_cast(properties_notes_next_activity_date as timestamp) as notes_next_activity_at,

        -- attribution fields
        cast(properties_hs_analytics_source as string) as analytics_source,
        cast(properties_hs_analytics_source_data_1 as string) as analytics_source_data_1,
        cast(properties_hs_analytics_source_data_2 as string) as analytics_source_data_2,
        cast(properties_hs_analytics_latest_source as string) as analytics_latest_source,
        cast(properties_hs_analytics_latest_source_data_1 as string) as analytics_latest_source_data_1,
        cast(properties_hs_analytics_latest_source_data_2 as string) as analytics_latest_source_data_2,

        -- retained raw fields
        properties as properties_json,
        line_items as line_items_json

    from source

),

deduplicated as (

    select
        *
    from renamed
    qualify row_number() over (
        partition by deal_id
        order by
            airbyte_extracted_at desc,
            updated_at desc,
            hs_last_modified_at desc
    ) = 1

)

select
    *
from deduplicated