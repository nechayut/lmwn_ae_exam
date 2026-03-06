 {{ config(
    materialized='table',
    schema='reports',
    alias='report_driver_performance',
    tags=['report','fleet']
) }}       
        

{% set status_type = [ 'completed',
                        'created',
                        'accepted',
                        'failed',
                        'canceled',
                        'picked_up']

%}

with source as(select driver_id,
        vehicle_type,
        delivery_zone,
        count(order_status) no_of_assign,
        sum(case when lower(order_status) = 'completed' then 1 else 0 end) as no_of_complete,
        cast(avg(Responsiveness_minute) as decimal(10,2)) responsiveness_minute,
        cast(avg(transport_minute) as decimal(10,2)) transport_minute,
        sum(case when is_late_delivery = true then 1 else 0 end) as late_delivery_count,
        cast(avg(csat_score) as decimal(10,2)) rating,
        count(csat_score) customer_issue_count
from {{ ref('model_dm_order_mapping_driver') }} 
group by driver_id,vehicle_type,delivery_zone )

select 
        driver_id,
        vehicle_type,
        delivery_zone,
        no_of_assign,
        no_of_complete,
        cast(no_of_complete/no_of_assign*100 as decimal(10,2)) complete_rate,
        responsiveness_minute,
        transport_minute,
        late_delivery_count,
        cast(late_delivery_count/no_of_assign*100 as decimal(10,2)) late_delivery_rate,
        rating,
        customer_issue_count,
        current_timestamp as report_load_dt 
from source