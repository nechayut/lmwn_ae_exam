{{ config(
    materialized='incremental',

    schema='dwh',
    unique_key='log_id',
    alias='model_fact_order_status',
    tags=['fact']
) }}


select
    log_id,
    order_id,
    status,
    status_datetime,
    updated_by,
    current_timestamp as dwh_load_dt
from {{source('raw','order_log_incentive_sessions_order_status_logs')}} 

{% if is_incremental() %}
where status_datetime >= (
  select coalesce(max(status_datetime), timestamp '1900-01-01') from {{ this }}
)
{% endif %}
