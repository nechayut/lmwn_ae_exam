 {{ config(
    materialized='table',
    schema='reports',
    alias='complaint_summary',
    tags=['report','customer service']
) }}       
        


select 
        issue_date,
        issue_type,
        count(distinct ticket_id) issue_volume,
        cast(avg(resolution_time_hour) as decimal(10,2)) avg_resolution_time_hour,
        sum(case when is_unresolved = true then 1 else 0 end) unresolved_volume,
        sum(case when is_escalated_tickets = true then 1 else 0 end) escalated_tickets_volume,
        sum(compensation_amount) total_refund,
        cast(avg(compensation_amount) as decimal(10,2)) avg_refund_per_ticket,
        cast(sum(case when is_escalated_tickets = true then 1 else 0 end)/count(distinct ticket_id)*100 as decimal(10,2)) percent_refund_rate,
        current_timestamp as report_load_dt
from {{ ref('model_dm_support_ticket_detail') }}
group by issue_date,issue_type
