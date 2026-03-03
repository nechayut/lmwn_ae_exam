 {{ config(
    materialized='table',
    schema='reports',
    alias='driver_performance_report',
    tags=['report','marketing']
) }}       
        

{% set status_type = [ 'completed',
                        'created',
                        'accepted',
                        'failed',
                        'canceled',
                        'picked_up']

%}

with order_status as (SELECT
                            order_id,
                            MAX(CASE WHEN status = 'completed' THEN status_datetime END) AS 'completed',
                            MAX(CASE WHEN status = 'created' THEN status_datetime END) AS 'created',
                            MAX(CASE WHEN status = 'accepted' THEN status_datetime END) AS 'accepted',
                            MAX(CASE WHEN status = 'canceled' THEN status_datetime END) AS 'canceled',
                            MAX(CASE WHEN status = 'failed' THEN status_datetime END) AS 'failed',
                            MAX(CASE WHEN status = 'picked_up' THEN status_datetime END) AS 'picked_up'
                        FROM {{ ref('fact_order_status') }}
                        GROUP BY order_id
                        ),
                
                support_ticket as ( select order_id ,csat_score from {{ ref('fact_support_ticket') }} ),

                drivers as ( select driver_id,vehicle_type,region from {{ ref('dim_drivers') }} ) 

SELECT ot.driver_id,
        vehicle_type,
        delivery_zone,
        count(order_status) no_of_assign,
        sum(case when lower(order_status) = 'completed' then 1 else 0 end) as no_of_complete,
        cast(avg(date_diff('minute',created,accepted)) as decimal(10,2)) Responsiveness_minute,
        cast(avg(date_diff('minute',picked_up,completed)) as decimal(10,2)) transport_minute,
        sum(case when is_late_delivery = true then 1 else 0 end) as late_delivery_count,
        cast(avg(csat_score) as decimal(10,2)) rating,
        count(csat_score) customer_issue_count,
        current_timestamp as report_load_dt
FROM {{ ref('fact_order_transactions') }} ot
left join order_status os on ot.order_id = os.order_id
left join support_ticket st on ot.order_id = st.order_id
left join drivers d on ot.driver_id = d.driver_id
group by ot.driver_id,vehicle_type,delivery_zone 