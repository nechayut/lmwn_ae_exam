{{ config(
    materialized='table',
    schema='dwh',
    alias='dim_restaurants',
    tags=['dim','type1']
) }}

select
    restaurant_id,
    name,
    category,
    city,
    average_rating,
    active_status,
    prep_time_min,
    current_timestamp as dwh_load_dt
from {{source('raw','restaurants_master')}}