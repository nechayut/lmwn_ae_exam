{{ config(
    materialized='incremental',
    unique_key='order_id',
    schema='dm',
    alias='model_dm_order_mapping_customer_mapping_campaign',
    tags=['mart','marketing']
) }}

with order_sequence_after_validate as (
                select order_id,
                        row_number() over(partition by customer_id order by order_datetime) customer_order_sequence_after_validate,
                        from {{ ref('model_fact_order_transactions') }}
                        where customer_pre_signup_order = false
        )
        select 
        o.order_id,
        o.order_datetime,
        o.order_status,
        o.delivery_zone,
        o.total_amount,
        o.payment_method,
        o.is_late_delivery,
        o.customer_id,
        row_number() over(partition by o.customer_id order by order_datetime) customer_order_sequence,
        oav.customer_order_sequence_after_validate,
        cus.signup_date ,
        cus.referral_source ,
        cus.birth_year ,
        cus.gender  ,
        r.category restaurant_category,
        r.average_rating,
        camp.campaign_id ,
        camp.interaction_datetime ,
        camp.event_type ,
        camp.platform ,
        camp.device_type ,
        camp.ad_cost    ,
        camp.is_new_customer ,
        camp.customer_pre_signup_order ,
        current_timestamp as dm_load_dt
        from {{ ref('model_fact_order_transactions') }} o
        inner join {{ ref('model_dim_customers') }} cus on o.customer_sk = cus.customer_sk
        left join {{ ref('model_fact_campaign_interactions') }} camp on o.order_id = camp.order_id
        left join order_sequence_after_validate oav on o.order_id = oav.order_id
        left join {{ ref('model_dim_restaurants') }} r on o.restaurant_id = r.restaurant_id  

{% if is_incremental() %}
  where o.dwh_load_dt > (
    select coalesce(max(dm_load_dt), timestamp '1900-01-01') from {{ this }}
)
{% endif %}

