 {{ config(
    materialized='view',
    schema='reports',
    alias='report_retargeting_performance',
    tags=['report','marketing']
) }}       
        
        
with retargeting as(
        select 
                campaign_id,
                objective,
                channel 
        from {{ ref('model_dim_campaign') }} where campaign_type = 'retargeting'
),
day_gap as (
        select 
                rcc.campaign_objective,
                customer_segment,
                cast(avg(time_gap_original_and_returning_orders) as decimal(10,2)) avg_day_gap_original_and_returning_orders,
                cast(avg(time_gap_returning_and_repeat_orders ) as decimal(10,2)) avg_day_gap_returning_and_repeat_orders,
                sum( case when is_order_after_retargeting = true then 1 else 0 end) retention_count
        from {{ ref('model_dm_retargeting_customer_conversion_time_gap') }} rcc
        left join {{ ref('model_dim_customers') }} cus on rcc.customer_id = cus.customer_id
        group by rcc.campaign_objective,customer_segment
),

source as (
        select 
            objective campaign_objective,
            cus.customer_segment,
            count(distinct dmo.customer_id) unique_targeted_count,
            count(distinct case when lower(event_type) = 'conversion' then dmo.customer_id else null end) unique_returned_count,
            cast(sum(case when lower(event_type) = 'conversion' then dmo.total_amount else null end) as decimal(10,2)) total_spend_by_retargeted_customers
        from {{ ref('model_dm_order_mapping_customer_mapping_campaign') }} dmo
        inner join retargeting r on dmo.campaign_id = r.campaign_id
        left join {{ ref('model_dim_customers') }} cus on dmo.customer_id = cus.customer_id
        group by objective,cus.customer_segment)
        
select 
        s.*,
        dg.avg_day_gap_original_and_returning_orders ,
        dg.avg_day_gap_returning_and_repeat_orders ,
        dg.retention_count,
        cast(dg.retention_count/s.unique_targeted_count*100 as decimal(10,2)) percent_retention_rate,
        current_timestamp as report_load_dt
from source s        
left join day_gap dg on s.campaign_objective = dg.campaign_objective
                and s.customer_segment = dg.customer_segment

