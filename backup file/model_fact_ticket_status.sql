{{ config(
    -- materialized='incremental',
    materialized='view',
    schema='dwh',
    -- unique_key='log_id',
    alias='model_fact_ticket_status',
    tags=['fact']
) }}

select
    log_id,
    ticket_id,
    status,
    status_datetime,
    agent_id,
    current_timestamp as dwh_load_dt
from {{source('raw','support_ticket_status_logs')}} o


-- {% if is_incremental() %}
-- where status_datetime >= (
--   select coalesce(max(status_datetime), timestamp '1900-01-01') from {{ this }}
-- )
-- {% endif %}
