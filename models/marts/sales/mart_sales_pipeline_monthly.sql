{{
    config(
        materialized = 'incremental',
        unique_key = 'created_month',
        incremental_strategy = 'merge',
        partition_by = {
            "field": "created_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = ["created_month"]
    )
}}

with pipeline as (

    select
        *
    from {{ ref('int_hubspot_deals_enriched') }}

    {% if is_incremental() %}
        where created_month >= (
            select date_sub(max(created_month), interval 1 month)
            from {{ this }}
        )
    {% endif %}

),

monthly_summary as (

    select
        created_month,

        count(*) as deals_created,

        countif(open_closed_status = 'Open') as open_deals,
        countif(open_closed_status = 'Closed') as closed_deals,

        countif(deal_status = 'Closed Won') as won_deals,
        countif(deal_status = 'Closed Lost') as lost_deals,
        countif(deal_status = 'Closed Other') as closed_other_deals,

        sum(effective_amount) as total_pipeline_value,
        sum(weighted_pipeline_amount) as weighted_pipeline_value,

        avg(nullif(effective_amount, 0)) as average_deal_value,
        avg(deal_age_days) as average_deal_age_days,
        avg(days_since_last_contact) as average_days_since_last_contact,

        safe_divide(
            countif(deal_status = 'Closed Won'),
            countif(open_closed_status = 'Closed')
        ) as win_rate,

        safe_divide(
            countif(deal_status = 'Closed Lost'),
            countif(open_closed_status = 'Closed')
        ) as loss_rate,

        current_timestamp() as mart_updated_at

    from pipeline

    group by
        created_month

)

select
    *
from monthly_summary