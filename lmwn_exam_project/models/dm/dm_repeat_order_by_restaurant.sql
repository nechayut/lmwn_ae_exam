{{ config(
    materialized='view',
    schema='dm',
    alias='dm_repeat_order_by_restaurant',
    tags=['dm','customer service']
) }}

with unique_customer as (
        select 
            restaurant_id,
            count(distinct customer_id) unique_customer
        from {{ ref('fact_order_transactions') }} 
        group by restaurant_id
        ),

unique_customer_repeat_order as(
        select 
            restaurant_id,
            count(distinct customer_id ) unique_customer_repeat_order from (
                                                                                select 
                                                                                    restaurant_id,
                                                                                    customer_id,
                                                                                from {{ ref('fact_order_transactions') }}
                                                                                group by restaurant_id,customer_id
                                                                                having count(*) > 1
                                                                                )
        group by restaurant_id
        )
    
select  
    uc.*,
    unique_customer_repeat_order,
    cast(unique_customer_repeat_order/unique_customer*100 as decimal(10,2)) repeat_order_rate
from unique_customer uc 
left join unique_customer_repeat_order ucro 
on uc.restaurant_id = ucro.restaurant_id