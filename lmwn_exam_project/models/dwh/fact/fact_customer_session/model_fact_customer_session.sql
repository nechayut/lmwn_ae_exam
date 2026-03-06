{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='session_id',
    alias='model_fact_customer_session',
    tags=['fact']
) }}
with customers_earlies as (
  select * from (select customer_sk,
        customer_id,
        effective_start,
        effective_end,
        row_number() over(partition by customer_id order by effective_start) rn  from {{ ref('model_dim_customers') }} ) c_earlies
        where rn = 1)

select
    coalesce(c.customer_sk,c_earlies.customer_sk) customer_sk,
    o.session_id,
    o.customer_id,
    o.session_start,
    o.session_end,
    o.device_type,
    o.os_version,
    o.app_version,
    o.location,
    case
      when c.customer_sk is null then True else False
    end as customer_pre_signup_order,
    current_timestamp as dwh_load_dt
from {{source('raw','order_log_incentive_sessions_customer_app_sessions')}} o
left join {{ ref('model_dim_customers') }} c
  on o.customer_id = c.customer_id
 and o.session_start between c.effective_start and c.effective_end
left join customers_earlies c_earlies
  on o.customer_id = c_earlies.customer_id

{% if is_incremental() %}
where session_start >= (
  select coalesce(max(session_start), timestamp '1900-01-01') from {{ this }}
)
{% endif %}

