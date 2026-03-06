{{ config(
    materialized='incremental',
    unique_key='order_id',
    schema='dm',
    alias='model_dm_order_mapping_driver',
    tags=['mart','marketing']
) }}


with order_status as (SELECT
                                    order_id,
                                    MAX(CASE WHEN status = 'created' 
                                             THEN status_datetime END) AS created_ts,
                                    MAX(CASE WHEN status = 'accepted' 
                                             THEN status_datetime END) AS accepted_ts,
                                    MAX(CASE WHEN status = 'picked_up' 
                                             THEN status_datetime END) AS picked_up_ts,
                                    MAX(CASE WHEN status = 'completed' 
                                             THEN status_datetime END) AS completed_ts,
                                    MAX(CASE WHEN status = 'failed' 
                                             THEN status_datetime END) AS failed_ts,
                                    MAX(CASE WHEN status = 'canceled' 
                                             THEN status_datetime END) AS canceled_ts
                                FROM {{ ref('model_fact_order_status') }}
                                GROUP BY order_id
                                ),
                                support_ticket as (
                                    select order_id ,csat_score from {{ ref('model_fact_support_ticket') }}
                                ),
                                drivers as (
                                    select driver_sk,vehicle_type,region from {{ ref('model_dim_drivers') }}
                                ) 
        SELECT 
                ot.order_id,
                customer_id, 
                restaurant_id, 
                driver_id, 
                order_datetime,    
                pickup_datetime,   
                delivery_datetime,  
                order_status, 
                delivery_zone, 
                total_amount, 
                payment_method, 
                is_late_delivery, 
                delivery_distance_km, 
                customer_pre_signup_order, 
                driver_pre_signup_order, 
                created_ts,      
                accepted_ts,     
                picked_up_ts,     
                completed_ts,     
                failed_ts, 
                canceled_ts,     
                csat_score, 
                vehicle_type, 
                region,  
                cast(date_diff('minute',created_ts,accepted_ts) as decimal(10,2)) Responsiveness_minute,
                cast(date_diff('minute',picked_up_ts,completed_ts) as decimal(10,2)) transport_minute,
                cast(date_diff('minute',created_ts,completed_ts) as decimal(10,2)) delivery_minute,
                current_timestamp as dm_load_dt
        FROM {{ ref('model_fact_order_transactions') }} ot
        left join order_status os on ot.order_id = os.order_id
        left join support_ticket st on ot.order_id = st.order_id
        left join drivers d on ot.driver_sk = d.driver_sk

{% if is_incremental() %}
  where ot.dwh_load_dt > (
    select coalesce(max(dm_load_dt), timestamp '1900-01-01') from {{ this }}
)
{% endif %}

