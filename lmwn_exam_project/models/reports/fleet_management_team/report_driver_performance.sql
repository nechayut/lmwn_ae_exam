 {{ config(
    materialized='table',
    schema='reports',
    alias='driver_performance_report',
    tags=['report','fleet']
) }}       
        

{% set status_type = [ 'completed',
                        'created',
                        'accepted',
                        'failed',
                        'canceled',
                        'picked_up']

%}

select driver_id,
        vehicle_type,
        delivery_zone,
        count(order_status) no_of_assign,
        sum(case when lower(order_status) = 'completed' then 1 else 0 end) as no_of_complete,
        cast(avg(Responsiveness_minute) as decimal(10,2)) Responsiveness_minute,
        cast(avg(transport_minute) as decimal(10,2)) transport_minute,
        sum(case when is_late_delivery = true then 1 else 0 end) as late_delivery_count,
        cast(avg(csat_score) as decimal(10,2)) rating,
        count(csat_score) customer_issue_count,
        current_timestamp as report_load_dt 
from {{ ref('model_dm_order_mapping_driver') }} 
group by driver_id,vehicle_type,delivery_zone 