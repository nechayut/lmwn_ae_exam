{{ config(
    materialized='incremental',
    unique_key='driver_sk'
) }}


with snap as (
  select
    dbt_scd_id as driver_sk,
    driver_id,
    join_date,
    vehicle_type,
    region,
    active_status,
    driver_rating,
    bonus_tier,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('drivers_stg') }}
),
rank as ( 
  select 
    *,
    row_number() over(partition by driver_id order by dbt_valid_from) rn 
  from snap)

select 
    driver_sk,
    driver_id,
    join_date,
    vehicle_type,
    region,
    active_status,
    driver_rating,
    bonus_tier,
      case
        when rn = 1 
        then cast(join_date as timestamp)
        else dbt_valid_from 
      end as effective_start,
  coalesce(dbt_valid_to,timestamp '9999-12-31 00:00:00') as effective_end,
  current_timestamp as dwh_load_dt
from rank
