{{ config(
    materialized='incremental',
    unique_key=['interaction_date','campaign_id','platform'],
    schema='dm',
    alias='dm_campaign_effectiveness_daily',
    tags=['mart','marketing']
) }}

select 
    cast(interaction_datetime as date) interaction_date,
    campaign_id,
    platform,
    count(event_type) impressions,
    sum(case when lower(event_type) in ('click','conversion') then 1 else 0 end) as clicks,
    sum(case when lower(event_type) = 'conversion' then 1 else 0 end) as conversions,
    count(distinct case when event_type in ('click','conversion') then customer_id end) as unique_clickers,
    count(distinct case when event_type = 'conversion' then customer_id end) as purchasers,
    cast(sum(ad_cost) as decimal(10,2)) total_ads_cost,
    cast(sum(case when event_type = 'conversion' then revenue else 0 end) as decimal(10,2)) total_revenue_from_ads,
    cast(sum(revenue) as decimal(10,2)) total_revenue,
    current_timestamp as dm_load_dt
from {{ ref('fact_campaign_interactions') }}

{% if is_incremental() %}
  where dwh_load_dt > (
    select coalesce(max(dm_load_dt), timestamp '1900-01-01') from {{ this }}
)
{% endif %}

group by interaction_date ,campaign_id,platform 