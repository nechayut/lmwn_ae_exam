{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='session_id',
    alias='fact_campaign_interactions',
    tags=['fact']
) }}
with customers_earlies as (
  select * from (select customer_sk,
        customer_id,
        effective_start,
        effective_end,
        row_number() over(partition by customer_id order by effective_start) rn  from {{ ref('dim_customers') }} ) c_earlies
        where rn = 1)

select
    coalesce(c.customer_sk,c_earlies.customer_sk) customer_sk,
    o.interaction_id,
    o.campaign_id,
    o.customer_id,
    o.interaction_datetime,
    o.event_type,
    o.platform,
    o.device_type,
    cast(o.ad_cost as DECIMAL(10, 2)) ad_cost,
    o.order_id,
    o.is_new_customer,
    cast(o.revenue as DECIMAL(10, 2)) revenue,
    o.session_id,
    case
      when c.customer_sk is null then True else False
    end as customer_pre_signup_order,
    current_timestamp as dwh_load_dt
from {{source('raw','campaign_interactions')}} o
left join {{ ref('dim_customers') }} c
  on o.customer_id = c.customer_id
 and o.interaction_datetime between c.effective_start and c.effective_end
left join customers_earlies c_earlies
  on o.customer_id = c_earlies.customer_id

{% if is_incremental() %}
where interaction_datetime >= (
  select coalesce(max(interaction_datetime), timestamp '1900-01-01') from {{ this }}
)
{% endif %}
