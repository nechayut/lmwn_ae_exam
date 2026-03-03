{{ config(
    materialized='incremental',
    unique_key=['report_month','campaign_id','platform'],
    schema='dm',
    alias='dm_campaign_effectiveness_monthly',
    tags=['mart','marketing']
) }}

with source as (
    select * from {{ ref('dm_order_mapping_customer_mapping_campaign') }}
    where campaign_id is not null
)
select 
    date_trunc('month',interaction_datetime) report_month,
    campaign_id,
    platform,
    count(event_type) impressions,
    sum(case when lower(event_type) in ('click','conversion') then 1 else 0 end) as clicks,
    sum(case when lower(event_type) = 'conversion' then 1 else 0 end) as conversions,
    count(distinct case when event_type in ('click','conversion') then customer_id end) as unique_clickers,
    count(distinct case when event_type = 'conversion' then customer_id end) as purchasers,
    cast(sum(ad_cost) as decimal(10,2)) ads_spend,
    cast(sum(case when event_type = 'conversion' then total_amount else 0 end) as decimal(10,2)) attributed_revenue,
    cast(sum(total_amount) as decimal(10,2)) total_revenue,
    current_timestamp as dm_load_dt
from source

{% if is_incremental() %}
  where dm_load_dt > (
    select coalesce(max(dm_load_dt), timestamp '1900-01-01') from {{ this }}
) and campaign_id is not null
{% endif %}

group by report_month ,campaign_id,platform 
