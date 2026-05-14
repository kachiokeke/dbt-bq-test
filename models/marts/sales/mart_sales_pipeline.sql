with deals as (

    select
        *
    from {{ ref('int_hubspot_deals_enriched') }}

),

pipeline_summary as (

    select
        -- date dimensions
        created_month,
        close_month,

        -- pipeline dimensions
        pipeline_id,
        deal_stage_id,
        deal_status,
        open_closed_status,

        -- source dimensions
        deal_source,
        analytics_source,
        analytics_latest_source,

        -- company/project context
        project_type,

        -- deal counts
        count(*) as deal_count,

        countif(open_closed_status = 'Open') as open_deal_count,
        countif(open_closed_status = 'Closed') as closed_deal_count,

        countif(deal_status = 'Closed Won') as won_deal_count,
        countif(deal_status = 'Closed Lost') as lost_deal_count,
        countif(deal_status = 'Closed Other') as closed_other_deal_count,

        -- data completeness counts
        countif(has_amount = true) as deals_with_amount_count,
        countif(has_company = true) as deals_with_company_count,
        countif(has_company_association = true) as deals_with_company_association_count,
        countif(has_close_date = true) as deals_with_close_date_count,

        -- amount metrics
        sum(effective_amount) as total_effective_amount,
        sum(weighted_pipeline_amount) as total_weighted_pipeline_amount,

        avg(nullif(effective_amount, 0)) as average_deal_amount,

        -- open pipeline amount
        sum(
            case
                when open_closed_status = 'Open'
                then effective_amount
                else 0
            end
        ) as open_pipeline_amount,

        sum(
            case
                when open_closed_status = 'Open'
                then weighted_pipeline_amount
                else 0
            end
        ) as weighted_open_pipeline_amount,

        -- won/lost amount
        sum(
            case
                when deal_status = 'Closed Won'
                then effective_amount
                else 0
            end
        ) as won_amount,

        sum(
            case
                when deal_status = 'Closed Lost'
                then effective_amount
                else 0
            end
        ) as lost_amount,

        -- lifecycle metrics
        avg(deal_age_days) as average_deal_age_days,
        avg(created_to_close_days) as average_created_to_close_days,
        avg(days_since_last_contact) as average_days_since_last_contact,

        -- simple conversion rates
        safe_divide(
            countif(deal_status = 'Closed Won'),
            countif(open_closed_status = 'Closed')
        ) as closed_won_rate,

        safe_divide(
            countif(deal_status = 'Closed Lost'),
            countif(open_closed_status = 'Closed')
        ) as closed_lost_rate,

        -- metadata
        current_timestamp() as mart_updated_at

    from deals

    group by
        created_month,
        close_month,
        pipeline_id,
        deal_stage_id,
        deal_status,
        open_closed_status,
        deal_source,
        analytics_source,
        analytics_latest_source,
        project_type

)

select
    *
from pipeline_summary