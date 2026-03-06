{{ config(
    materialized='view',
    schema='dwh',
    alias='model_dim_campaign',
    tags=['dim','type1']
) }}

select
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    campaign_type,
    objective,
    channel,
    budget,
    cost_model,
    targeting_strategy,
    is_active,
    current_timestamp as dwh_load_dt
from {{source('raw','campaign_master')}}
