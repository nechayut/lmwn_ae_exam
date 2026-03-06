 {{ config(

    materialized='view',
    schema='reports',
    alias='report_driver_incentive_impact',
    tags=['report','fleet']
) }}       
        

with metric_1 as (
                select 
                        cast(avg(total_amount) as decimal(10,2)) avg_total_amount,
                        cast(avg(delivery_minute) as decimal(10,2)) avg_delivery_minute,
                        cast(sum(case when lower(order_status) = 'completed' then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_complete_rate,
                        cast(sum(case when lower(order_status) = 'canceled' then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_canceled_rate,
                        cast(sum(case when is_late_delivery = true then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_late_order_count,
                        count(distinct driver_id) driver_count
                from {{ ref('model_dm_order_mapping_driver') }} 
        
                ),
        metric_2 as (
                select cast(avg(complete_order_count) as decimal(10,2)) avg_complete_order_per_driver_per_day 
                from (
                        select 
                                driver_id,
                                cast(order_datetime as date) order_date,
                                sum(case when lower(order_status) = 'completed' then 1 else 0 end) complete_order_count
                        from {{ ref('model_dm_order_mapping_driver') }} 
                group by driver_id,order_date )

                )
        
select 
        incentive_program,
        avg_total_amount_incentive_period ,
        avg_total_amount,
        bonus_amount,
        avg_bonus_amount,
        cast(driver_count_incentive_period/driver_count*100 as decimal(10,2)) participation_rate,
        complete_order_count_incentive_period completed_deliveries_incentive_period,
        avg_delivery_minute_incentive_period ,
        avg_delivery_minute ,
        percent_complete_rate_incentive_period ,
        percent_complete_rate ,
        percent_canceled_rate_incentive_period ,
        percent_canceled_rate,
        percent_late_order_count_incentive_period ,
        percent_late_order_count,         
        avg_actual_deliveries_per_driver_incentive_period ,
        avg_complete_order_per_driver_per_day avg_actual_deliveries_per_driver,
        current_timestamp as report_load_dt
from {{ ref('model_dm_incentive_detail_show_in_order_transaction') }}  did
left join metric_1 m1 on true
left join metric_2 m2 on true