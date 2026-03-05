{{ config(
    materialized='view',
    schema='dm',
    alias='dm_support_ticket_detail',
    tags=['dm','customer service']
) }}

select 
        opened_datetime issue_datetime,
        date_trunc('day' ,opened_datetime) issue_date,
        ticket_id,
        customer_id, 
        driver_id,
        restaurant_id,
        issue_type,
        issue_sub_type,
        date_diff('hour',opened_datetime , resolved_datetime) resolution_time_hour,
        csat_score,
        compensation_amount,
        case when lower(status) != 'resolved' then true else false end is_unresolved,
        case when compensation_amount > 0 then true else false end is_escalated_tickets,
from {{ ref('fact_support_ticket') }}