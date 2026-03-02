{% snapshot customers_stg %}

{{ config(
    target_schema='staging',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_segment','status','preferred_device'],
    alias='customers_stg',
    tags=['dim','type2']
) }}

select
    customer_id,
    signup_date,
    customer_segment,
    status,
    referral_source,
    birth_year,
    gender,
    preferred_device,
    current_timestamp as dwh_load_dt
from {{source('raw','customers_master')}}


{% endsnapshot %}


