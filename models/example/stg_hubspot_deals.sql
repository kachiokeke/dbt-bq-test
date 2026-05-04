{{ config(
    materialized = 'view'
) }}

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
        _airbyte_meta as airbyte_meta_json,

        -- core hubspot identifiers
        cast(id as string) as deal_id,
        safe_cast(properties_hs_object_id as string) as hs_object_id,

        -- status flags
        safe_cast(archived as bool) as is_archived,
        safe_cast(properties_hs_is_closed as bool) as is_closed,
        safe_cast(properties_hs_is_closed_won as bool) as is_closed_won,
        safe_cast(properties_hs_is_closed_lost as bool) as is_closed_lost,
        safe_cast(properties_hs_is_open_count as int64) as is_open_count,
        safe_cast(properties_hs_is_closed_count as int64) as is_closed_count,
        safe_cast(properties_hs_is_stalled as bool) as is_stalled,
        safe_cast(properties_hs_is_in_first_deal_stage as bool) as is_in_first_deal_stage,

        -- association fields
        contacts as contact_ids_json,
        companies as company_ids_json,
        safe_cast(json_value(companies, '$[0]') as string) as primary_company_id_from_array,
        safe_cast(properties_hs_primary_associated_company as string) as primary_company_id,

        -- timestamps from top-level fields
        safe_cast(createdAt as timestamp) as created_at,
        safe_cast(updatedAt as timestamp) as updated_at,

        -- timestamps from HubSpot properties
        safe_cast(properties_createdate as timestamp) as property_created_at,
        safe_cast(properties_hs_createdate as timestamp) as hs_created_at,
        safe_cast(properties_hs_lastmodifieddate as timestamp) as hs_last_modified_at,
        safe_cast(properties_closedate as timestamp) as close_at,
        date(safe_cast(properties_closedate as timestamp)) as close_date,

        -- deal details
        cast(properties_dealname as string) as deal_name,
        cast(properties_dealtype as string) as deal_type,
        cast(properties_pipeline as string) as pipeline_id,
        cast(properties_dealstage as string) as deal_stage_id,
        cast(properties_deal_source as string) as deal_source,
        cast(properties_description as string) as deal_description,
        cast(properties_hs_priority as string) as priority,

        -- company/client details
        cast(properties_company_name as string) as company_name,
        cast(properties_company_code__sync_ as string) as company_code,
        cast(properties_icp_category__sync_ as string) as icp_category,
        cast(properties_icp_endclient as string) as icp_end_client,

        -- ownership
        cast(properties_hubspot_owner_id as string) as hubspot_owner_id,
        cast(properties_hubspot_team_id as string) as hubspot_team_id,
        cast(properties_hs_all_owner_ids as string) as all_owner_ids,
        cast(properties_hs_all_team_ids as string) as all_team_ids,
        cast(properties_hs_owning_teams as string) as owning_teams,

        -- project/SOW fields
        cast(properties_project_code as string) as project_code,
        cast(properties_project_name as string) as project_name,
        cast(properties_project_type as string) as project_type,
        cast(properties_sow as string) as sow,
        cast(properties_sow_link as string) as sow_link,
        cast(properties_sow_status as string) as sow_status,
        cast(properties_spoc as string) as spoc,
        cast(properties_t_client as string) as t_client,
        safe_cast(properties_date_sow_sent as date) as date_sow_sent,
        safe_cast(properties_estimated_kickoff_date as date) as estimated_kickoff_date,
        cast(properties_pre_kickoff_document_link as string) as pre_kickoff_document_link,
        cast(properties_send_data_to_productive as string) as send_data_to_productive,

        -- financial fields
        safe_cast(properties_amount as numeric) as amount,
        safe_cast(properties_amount_in_home_currency as numeric) as amount_in_home_currency,
        safe_cast(properties_hs_closed_amount as numeric) as closed_amount,
        safe_cast(properties_hs_closed_amount_in_home_currency as numeric) as closed_amount_in_home_currency,
        safe_cast(properties_hs_forecast_amount as numeric) as forecast_amount,
        safe_cast(properties_hs_projected_amount as numeric) as projected_amount,
        safe_cast(properties_hs_predicted_amount as numeric) as predicted_amount,
        safe_cast(properties_hs_weighted_pipeline_in_company_currency as numeric) as weighted_pipeline_amount,
        safe_cast(properties_hs_acv as numeric) as acv,
        safe_cast(properties_hs_arr as numeric) as arr,
        safe_cast(properties_hs_mrr as numeric) as mrr,
        safe_cast(properties_hs_tcv as numeric) as tcv,
        safe_cast(properties_hs_exchange_rate as numeric) as exchange_rate,

        -- probabilities / scoring
        safe_cast(properties_hs_deal_score as numeric) as deal_score,
        safe_cast(properties_hs_deal_stage_probability as numeric) as deal_stage_probability,
        safe_cast(properties_hs_forecast_probability as numeric) as forecast_probability,
        safe_cast(properties_hs_likelihood_to_close as numeric) as likelihood_to_close,

        -- lifecycle / velocity fields
        safe_cast(properties_days_to_close as int64) as days_to_close,
        safe_cast(properties_hs_days_to_close_raw as numeric) as days_to_close_raw,
        safe_cast(properties_deal_velocity as numeric) as deal_velocity,
        safe_cast(properties_hs_average_deal_owner_duration_in_current_stage as int64) as avg_owner_duration_current_stage_ms,

        -- activity fields
        safe_cast(properties_num_notes as int64) as num_notes,
        safe_cast(properties_num_contacted_notes as int64) as num_contacted_notes,
        safe_cast(properties_num_associated_contacts as int64) as num_associated_contacts,
        safe_cast(properties_hs_num_of_associated_line_items as int64) as num_associated_line_items,
        safe_cast(properties_hs_number_of_call_engagements as int64) as number_of_call_engagements,
        safe_cast(properties_hs_number_of_inbound_calls as int64) as number_of_inbound_calls,
        safe_cast(properties_hs_number_of_outbound_calls as int64) as number_of_outbound_calls,
        safe_cast(properties_hs_number_of_overdue_tasks as int64) as number_of_overdue_tasks,
        safe_cast(properties_hs_number_of_scheduled_meetings as int64) as number_of_scheduled_meetings,

        safe_cast(properties_notes_last_updated as timestamp) as notes_last_updated_at,
        safe_cast(properties_notes_last_contacted as timestamp) as notes_last_contacted_at,
        safe_cast(properties_notes_next_activity_date as timestamp) as notes_next_activity_at,
        cast(properties_hs_notes_next_activity_type as string) as notes_next_activity_type,
        safe_cast(properties_hs_latest_meeting_activity as timestamp) as latest_meeting_activity_at,
        safe_cast(properties_hs_next_meeting_start_time as timestamp) as next_meeting_start_at,
        cast(properties_hs_next_meeting_name as string) as next_meeting_name,

        -- sales/marketing attribution
        cast(properties_hs_analytics_source as string) as analytics_source,
        cast(properties_hs_analytics_source_data_1 as string) as analytics_source_data_1,
        cast(properties_hs_analytics_source_data_2 as string) as analytics_source_data_2,
        cast(properties_hs_analytics_latest_source as string) as analytics_latest_source,
        cast(properties_hs_analytics_latest_source_data_1 as string) as analytics_latest_source_data_1,
        cast(properties_hs_analytics_latest_source_data_2 as string) as analytics_latest_source_data_2,
        safe_cast(properties_hs_analytics_latest_source_timestamp as timestamp) as analytics_latest_source_at,
        cast(properties_hs_campaign as string) as campaign,

        -- lost/won reason fields
        cast(properties_close_lost_reason as string) as close_lost_reason,
        cast(properties_closed_lost_reason as string) as closed_lost_reason,
        cast(properties_closed_won_reason as string) as closed_won_reason,
        safe_cast(properties_hs_closed_won_date as timestamp) as closed_won_at,

        -- stage dates
        safe_cast(properties_hs_date_entered_current_stage as timestamp) as date_entered_current_stage_at,
        safe_cast(properties_hs_v2_date_entered_current_stage as timestamp) as v2_date_entered_current_stage_at,
        safe_cast(properties_hs_is_stalled_after_timestamp as timestamp) as stalled_after_at,

        safe_cast(properties_hs_date_entered_1057575733 as timestamp) as date_entered_stage_1057575733_at,
        safe_cast(properties_hs_date_entered_1057575734 as timestamp) as date_entered_stage_1057575734_at,
        safe_cast(properties_hs_date_entered_1057575735 as timestamp) as date_entered_stage_1057575735_at,
        safe_cast(properties_hs_date_entered_1150850673 as timestamp) as date_entered_stage_1150850673_at,
        safe_cast(properties_hs_date_entered_1150932571 as timestamp) as date_entered_stage_1150932571_at,
        safe_cast(properties_hs_date_entered_closedwon as timestamp) as date_entered_closed_won_at,
        safe_cast(properties_hs_date_entered_closedlost as timestamp) as date_entered_closed_lost_at,

        safe_cast(properties_hs_date_exited_1057575733 as timestamp) as date_exited_stage_1057575733_at,
        safe_cast(properties_hs_date_exited_1057575734 as timestamp) as date_exited_stage_1057575734_at,
        safe_cast(properties_hs_date_exited_1057575735 as timestamp) as date_exited_stage_1057575735_at,
        safe_cast(properties_hs_date_exited_1150850673 as timestamp) as date_exited_stage_1150850673_at,
        safe_cast(properties_hs_date_exited_1150932571 as timestamp) as date_exited_stage_1150932571_at,
        safe_cast(properties_hs_date_exited_closedwon as timestamp) as date_exited_closed_won_at,
        safe_cast(properties_hs_date_exited_closedlost as timestamp) as date_exited_closed_lost_at,

        -- useful raw json fields retained for later modeling
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