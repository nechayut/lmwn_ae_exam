 {{ config(
    materialized='table',
    schema='reports',
    alias='delivery_zone_heatmap_report',
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
        sum(driver_count) total_unique_driver_per_hour,
        sum(canceled_driver_count) total_unique_canceled_driver_per_hour ,
        cast(avg(order_count) as decimal(10,2)) avg_order_count,
        cast(avg(complete_order_count) as decimal(10,2)) avg_complete_order_count,
        cast(avg(canceled_order_count) as decimal(10,2)) avg_reject_order_count,
        cast(avg(driver_count)as decimal(10,2)) driver_count,
        cast(avg(canceled_driver_count)as decimal(10,2)) canceled_driver_count,
        cast(sum(late_order_count)/sum(order_count)*100 as decimal(10,2)) percent_late_delivery_rate,
        current_timestamp as report_load_dt
from {{ ref('dm_delivery_zone_by_hour') }}
group by delivery_zone,hour_slot

