{{ config(
    materialized='view',
    schema='dm',
    alias='model_dm_incentive_detail_show_in_order_transaction',
    tags=['dm','fleet']
) }}

with driver_incentive as (
                        SELECT 
                                driver_id ,
                                incentive_program ,
                                applied_date 
                        FROM {{ ref('model_fact_driver_incentive') }}
        ),
        incentive_order_show_in_order_transaction as (
                        SELECT 
                                omd.*,
                                incentive_program 
                        FROM {{ ref('model_dm_order_mapping_driver') }} omd
                        left join driver_incentive di on omd.driver_id = di.driver_id
                        and cast(omd.order_datetime as date) = di.applied_date
                        where incentive_program is not null),
        
        intensive_detail_measure as (
                        select  
                                incentive_program,
                                cast(avg(total_amount) as decimal(10,2)) avg_total_amount_incentive_period,
                                cast(avg(delivery_minute) as decimal(10,2)) avg_delivery_minute_incentive_period,
                                cast(sum(case when lower(order_status) = 'completed' then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_complete_rate_incentive_period,
                                cast(sum(case when lower(order_status) = 'canceled' then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_canceled_rate_incentive_period,
                                cast(sum(case when is_late_delivery = true then 1 else 0 end)/count(distinct order_id)*100 as decimal(10,2)) percent_late_order_count_incentive_period,

                        from incentive_order_show_in_order_transaction
                        group by incentive_program),
        
        intensive_detail as (
                        SELECT 
                                incentive_program,
                                count(distinct driver_id) driver_count_incentive_period,
                                cast(avg(actual_deliveries) as decimal(10,2)) avg_actual_deliveries_per_driver_incentive_period,
                                sum(actual_deliveries) complete_order_count_incentive_period,
                                cast(avg(bonus_amount) as decimal(10,2)) avg_bonus_amount,
                                cast(sum(bonus_amount) as decimal(10,2)) bonus_amount
                        FROM {{ ref('model_fact_driver_incentive') }}
                        group by incentive_program)
        
        select idm.*,driver_count_incentive_period,
        avg_actual_deliveries_per_driver_incentive_period,
        complete_order_count_incentive_period,
        avg_bonus_amount,
        bonus_amount  
        from intensive_detail_measure idm
        join intensive_detail id on idm.incentive_program = id.incentive_program