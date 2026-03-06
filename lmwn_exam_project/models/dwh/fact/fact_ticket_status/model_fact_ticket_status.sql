{{ config(
    materialized='view',
    schema='dwh',
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


