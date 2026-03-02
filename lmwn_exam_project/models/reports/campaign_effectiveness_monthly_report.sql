{{ config(
    materialized='table',
    schema='reports',
    alias='campaign_effectiveness_monthly_report',
    tags=['report','marketing']
) }}

with agg as (select *,
                cast((attributed_revenue::double / nullif(ads_spend, 0)) as decimal(10,4)) as return_on_ads_spend,
                cast((ads_spend::double / nullif(clicks, 0)) as decimal(10,4)) as cost_per_click,
                cast((conversions::double / nullif(clicks, 0)) as decimal(10,4)) as conversion_rate,
                cast((clicks::double / nullif(impressions, 0)) as decimal(10,4)) as click_through_rate,
                cast((ads_spend::double / nullif(purchasers, 0)) as decimal(10,4)) as cost_per_acquisition,
            from {{ ref('dm_campaign_effectiveness_monthly') }})
select r.report_month,
        r.campaign_id,
        d.campaign_name,
        d.campaign_type,
        r.platform,
        d.channel,
        d.objective,
        d.budget total_budget,
        r.ads_spend,
        r.attributed_revenue,
        r.return_on_ads_spend,
        r.cost_per_acquisition,
        r.impressions,
        r.clicks,
        r.conversions,
        r.click_through_rate,
        r.cost_per_click,
        r.conversion_rate,
        r.unique_clickers,
        r.purchasers 
from agg r
join {{ ref('dim_campaign') }} d on r.campaign_id = d.campaign_id