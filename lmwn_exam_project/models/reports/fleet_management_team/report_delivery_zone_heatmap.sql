 {{ config(
    materialized='table',
    schema='reports',
    alias='report_delivery_zone_heatmap',
    tags=['report','fleet']
) }}       
        


select delivery_zone,
        hour_slot,
        sum(order_count) total_order_count,
        sum(complete_order_count) total_complete_order_count,
        cast(sum(complete_order_count)/sum(order_count)*100 as decimal(10,2)) percent_complete_rate,
        cast(sum(avg_delivery_minute*complete_order_count)/sum(complete_order_count) as decimal(10,2)) avg_delivery_minute,
        sum(canceled_order_count) total_reject_order_count,
        cast(sum(canceled_order_count)/sum(order_count)*100 as decimal(10,2)) percent_reject_rate,
        sum(driver_count) sum_unique_driver_per_hour_per_day,
        sum(canceled_driver_count) sum_unique_canceled_driver_per_hour_per_day ,
        cast(avg(order_count) as decimal(10,2)) avg_order_per_hour_per_day,
        cast(avg(complete_order_count) as decimal(10,2)) avg_complete_order_per_hour_per_day,
        cast(avg(canceled_order_count) as decimal(10,2)) avg_reject_order_per_hour_per_day,
        cast(avg(driver_count)as decimal(10,2)) avg_driver_per_hour_per_day,
        cast(avg(canceled_driver_count)as decimal(10,2)) avg_canceled_driver_per_hour_per_day,
        cast(sum(late_order_count)/sum(order_count)*100 as decimal(10,2)) percent_late_delivery_rate,
        current_timestamp as report_load_dt
from {{ ref('model_dm_delivery_zone_by_hour') }}
group by delivery_zone,hour_slot

