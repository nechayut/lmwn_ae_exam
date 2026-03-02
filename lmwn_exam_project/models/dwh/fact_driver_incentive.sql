{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='log_id',
    alias='fact_driver_incentive',
    tags=['fact']
) }}
with driver_earlies as (
  select * from (select 
        driver_sk,
        driver_id,
        effective_start,
        effective_end,
        row_number() over(partition by driver_id order by effective_start) rn   from {{ ref('dim_drivers') }} ) d_earlies
        where rn = 1)

select
    coalesce(d.driver_sk,d_earlies.driver_sk) driver_sk,
    o.log_id,
    o.driver_id,
    o.incentive_program,
    o.bonus_amount,
    o.applied_date,
    o.delivery_target,
    o.actual_deliveries,
    o.bonus_qualified,
    o.region,
    case
      when d.driver_sk is null then True else False
    end as driver_pre_signup_order,
    current_timestamp as dwh_load_dt
from {{source('raw','order_log_incentive_sessions_driver_incentive_logs')}} o
left join {{ ref('dim_drivers') }} d
  on o.driver_id = d.driver_id
 and o.applied_date between d.effective_start and d.effective_end
left join driver_earlies d_earlies
  on o.driver_id = d_earlies.driver_id

{% if is_incremental() %}
where applied_date >= (
  select coalesce(max(applied_date), timestamp '1900-01-01') from {{ this }}
)
{% endif %}
