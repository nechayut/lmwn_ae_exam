{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='order_id',
    alias='fact_order_transactions',
    tags=['fact']
) }}
with customers_earlies as (
  select * from (select customer_sk,
        customer_id,
        effective_start,
        effective_end,
        row_number() over(partition by customer_id order by effective_start) rn  from {{ ref('dim_customers') }} ) c_earlies
        where rn = 1),
  driver_earlies as (
  select * from (select 
        driver_sk,
        driver_id,
        effective_start,
        effective_end,
        row_number() over(partition by driver_id order by effective_start) rn   from {{ ref('dim_drivers') }} ) d_earlies
        where rn = 1)

select
    coalesce(c.customer_sk,c_earlies.customer_sk) customer_sk,
    coalesce(d.driver_sk,d_earlies.driver_sk) driver_sk,
    o.order_id,
    o.customer_id,
    o.restaurant_id,
    o.driver_id,
    o.order_datetime,
    o.pickup_datetime,
    o.delivery_datetime,
    o.order_status,
    o.delivery_zone,
    o.total_amount,
    o.payment_method,
    o.is_late_delivery,
    o.delivery_distance_km,
    case
      when c.customer_sk is null then True else False
    end as customer_pre_signup_order,
    case
      when d.driver_sk is null then True else False
    end as driver_pre_signup_order,
    current_timestamp as dwh_load_dt
from {{source('raw','order_transactions')}} o
left join {{ ref('dim_customers') }} c
  on o.customer_id = c.customer_id
 and o.order_datetime between c.effective_start and c.effective_end
left join customers_earlies c_earlies
  on o.customer_id = c_earlies.customer_id
left join {{ ref('dim_drivers') }} d
  on o.driver_id = d.driver_id
 and o.order_datetime between d.effective_start and d.effective_end
left join driver_earlies d_earlies
  on o.driver_id = d_earlies.driver_id

{% if is_incremental() %}
where order_datetime >= (
  select coalesce(max(order_datetime), timestamp '1900-01-01') from {{ this }}
)
{% endif %}


