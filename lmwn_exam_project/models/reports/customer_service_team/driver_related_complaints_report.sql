 {{ config(
    materialized='table',
    schema='reports',
    alias='driver_related_complaints_report',
    tags=['report','customer service']
) }}       
        

with driver_issue as (
        select  
                driver_id,
                count(*) total_complaints_volume,
                sum(case when issue_sub_type = 'not_delivered' then 1 else 0 end) not_delivered_case,
                sum(case when issue_sub_type = 'late' then 1 else 0 end) late_case,
                sum(case when issue_sub_type = 'cold' then 1 else 0 end) cold_case,
                sum(case when issue_sub_type = 'wrong_item' then 1 else 0 end) wrong_item_case,
                sum(case when issue_sub_type = 'no_mask' then 1 else 0 end) no_mask_case,
                sum(case when issue_sub_type = 'rude' then 1 else 0 end) rude_case,
                cast(avg(resolution_time_hour)as decimal(10,2)) avg_resolution_time_hour,
                cast(avg(csat_score)as decimal(10,2)) customer_satisfaction_scores,
        from {{ ref('dm_support_ticket_detail') }} 
        where issue_type != 'payment'
        group by driver_id
        ),
        total_order as (
                select 
                        driver_id,
                        count(*) total_order_count
                from {{ ref('fact_order_transactions') }}
                group by driver_id
        )
select 
        di.*,
        dd.driver_rating,
        cast(total_complaints_volume/total_order_count as decimal(10,2)) complaints_ratio
from driver_issue di
left join {{ ref('dim_drivers') }} dd on di.driver_id = dd.driver_id
left join total_order t on di.driver_id = t.driver_id 