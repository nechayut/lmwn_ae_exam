 {{ config(
    materialized='table',
    schema='reports',
    alias='restaurant_quality_complaint_report',
    tags=['report','customer service']
) }}       
        

with restaurant_issue as(
        select  
                restaurant_id,
                count(*) total_complaints_volume,
                count(distinct customer_id) unique_customer_complaints_count,
                sum(case when issue_sub_type = 'wrong_item' then 1 else 0 end) wrong_item_case,
                sum(case when issue_sub_type = 'cold' then 1 else 0 end) cold_case,
                cast(avg(resolution_time_hour)as decimal(10,2)) avg_resolution_time_hour,
                cast(sum(compensation_amount) as decimal(10,2)) total_refund,
                cast(avg(compensation_amount) as decimal(10,2)) avg_refund_per_ticket,
        from {{ ref('model_dm_support_ticket_detail') }}
        where issue_type = 'food'
        group by restaurant_id

),
total_order as (
        select 
                restaurant_id,
                count(*) total_order_count
        from {{ ref('model_fact_order_transactions') }}
        group by restaurant_id
), 
first_issue as (
        select 
                customer_id,
                restaurant_id,
                min(issue_datetime) first_issue_datetime
        from {{ ref('model_dm_support_ticket_detail') }}
        where issue_type = 'food'
        group by customer_id ,restaurant_id
),        
customer_repeat_order as (
        select 
                ot.restaurant_id,
                count(distinct ot.customer_id) unique_customer_repeat_order_count
        from {{ ref('model_fact_order_transactions') }} ot
        inner join first_issue fi 
                on ot.customer_id = fi.customer_id
                and ot.restaurant_id = fi.restaurant_id
                and ot.order_datetime > fi.first_issue_datetime
        group by ot.restaurant_id
        )
           
select 
        ri.*,
        cast(total_complaints_volume/total_order_count as decimal(10,2)) complaints_ratio,
        cast(unique_customer_repeat_order_count/unique_customer_complaints_count*100 as decimal(10,2)) customer_repeat_rate_after_issue,
        ro.repeat_order_rate,
        current_timestamp as report_load_dt
from restaurant_issue ri
left join total_order t on ri.restaurant_id = t.restaurant_id
left join customer_repeat_order cro on ri.restaurant_id = cro.restaurant_id
left join {{ ref('model_dm_repeat_order_by_restaurant') }} ro on ri.restaurant_id = ro.restaurant_id