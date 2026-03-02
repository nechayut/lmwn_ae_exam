{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='ticket_id',
    alias='fact_support_ticket',
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
    s.ticket_id,
    s.order_id,
    s.customer_id,
    s.driver_id,
    s.restaurant_id,
    s.issue_type,
    s.issue_sub_type,
    s.channel,
    s.opened_datetime,
    s.resolved_datetime,
    s.status,
    s.csat_score,
    s.compensation_amount,
    s.resolved_by_agent_id,
    case
      when c.customer_sk is null then True else False
    end as customer_pre_signup_order,
    case
      when d.driver_sk is null then True else False
    end as driver_pre_signup_order,
    current_timestamp as dwh_load_dt
from {{source('raw','support_tickets')}} s
left join {{ ref('dim_customers') }} c
  on s.customer_id = c.customer_id
 and s.opened_datetime between c.effective_start and c.effective_end
left join customers_earlies c_earlies
  on s.customer_id = c_earlies.customer_id
left join {{ ref('dim_drivers') }} d
  on s.driver_id = d.driver_id
 and s.opened_datetime between d.effective_start and d.effective_end
left join driver_earlies d_earlies
  on s.driver_id = d_earlies.driver_id

{% if is_incremental() %}
where opened_datetime >= (
  select coalesce(max(opened_datetime), timestamp '1900-01-01') from {{ this }}
)
{% endif %}
