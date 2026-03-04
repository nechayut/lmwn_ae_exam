{{ config(
    materialized='table',
    schema='dm',
    alias='dm_delivery_zone_by_hour',
    tags=['dm','fleet']
) }}

select 
    delivery_zone,
    date_trunc('day', order_datetime) order_date,
    strftime(date_trunc('hour', order_datetime), '%H:%M') AS hour_slot,
    count(distinct order_id) order_count,
    sum(case when lower(order_status) = 'completed' then 1 else 0 end) complete_order_count,
    cast(avg(delivery_minute) as decimal(10,2)) avg_delivery_minute,
    sum(case when lower(order_status) = 'canceled' then 1 else 0 end) canceled_order_count,
    count(distinct driver_id) driver_count,
    count(distinct case when lower(order_status) = 'canceled' then driver_id else null end) canceled_driver_count,
    sum(case when is_late_delivery = true then 1 else 0 end) late_order_count,
    current_timestamp as dm_load_dt
from {{ ref('dm_order_mapping_driver') }}
group by delivery_zone,order_date,hour_slot
order by delivery_zone,order_date,hour_slot