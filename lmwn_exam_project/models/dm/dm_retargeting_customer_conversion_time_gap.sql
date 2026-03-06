{{ config(
    materialized='view',
    schema='dm',
    alias='dm_retargeting_customer_conversion_time_gap',
    tags=['dm','customer service']
) }}

with retargeting as(
select 
    campaign_id,
    objective,channel 
from {{ ref('dim_campaign') }}
where campaign_type = 'retargeting'
),
transaction as (
        select 
            customer_id,
            order_datetime 
        from {{ ref('dm_order_mapping_customer_mapping_campaign') }}
),
first_conversion as (
        select 
            objective,
            dmo.customer_id,
            min(order_datetime) first_conversion
        from {{ ref('dm_order_mapping_customer_mapping_campaign') }} dmo
        inner join retargeting r on dmo.campaign_id = r.campaign_id
        group by objective,dmo.customer_id
)
select 
        *,
        date_diff('day', last_order_datetime_before_conversion, first_conversion) time_gap_original_and_returning_orders,
        date_diff('day', first_conversion, first_order_datetime_after_conversion) time_gap_returning_and_repeat_orders
from (
        select 
            objective campaign_objective,
            fc.customer_id,
            first_conversion,
            max(tb.order_datetime) last_order_datetime_before_conversion,
            min(ta.order_datetime) first_order_datetime_after_conversion,
            case when max(tb.order_datetime) is not null then true else false end is_order_before_retargeting,
            case when min(ta.order_datetime) is not null then true else false end is_order_after_retargeting
        from first_conversion fc
        left join transaction tb on fc.customer_id = tb.customer_id
                and fc.first_conversion > tb.order_datetime
        left join transaction ta on fc.customer_id = ta.customer_id
                and fc.first_conversion < ta.order_datetime
        group by objective,fc.customer_id,first_conversion
)