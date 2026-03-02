{% snapshot drivers_stg %}

{{ config(
    target_schema='staging',
    unique_key='driver_id',
    strategy='check',
    check_cols=['vehicle_type','region','active_status','driver_rating','bonus_tier'],
    alias='drivers_stg',
    tags=['dim','type2']
) }}

select
    driver_id,
    join_date,
    vehicle_type,
    region,
    active_status,
    driver_rating,
    bonus_tier,
    current_timestamp as dwh_load_dt
from {{source('raw','drivers_master')}}

{% endsnapshot %}
