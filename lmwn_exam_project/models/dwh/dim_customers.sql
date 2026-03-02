{{ config(
    materialized='incremental',
    unique_key='customer_sk'
) }}

with snap as (
  select
    dbt_scd_id as customer_sk,
    customer_id,
    signup_date,
    customer_segment,
    status,
    referral_source,
    birth_year,
    gender,
    preferred_device,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('customers_stg') }} 
),
rank as ( 
  select 
    *,
    row_number() over(partition by customer_id order by dbt_valid_from) rn 
  from snap)

select 
      customer_sk,
      customer_id,
      signup_date,
      customer_segment,
      status,
      referral_source,
      birth_year,
      gender,
      preferred_device,
      case
        when rn = 1 
        then cast(signup_date as timestamp)
        else dbt_valid_from 
      end as effective_start,
  coalesce(dbt_valid_to,timestamp '9999-12-31 00:00:00') as effective_end,
  current_timestamp as dwh_load_dt
from rank
