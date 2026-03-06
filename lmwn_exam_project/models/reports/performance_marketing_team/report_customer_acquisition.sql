 {{ config(
    materialized='table',
    schema='reports',
    alias='report_customer_acquisition',
    tags=['report','marketing']
) }}       
        
        
with first_last_session_start as (
        SELECT customer_id,
                date_trunc('day',max(session_start)) last_session_date,
                date_trunc('day',min(session_start)) first_session_date,
        from {{ ref('model_fact_customer_session') }}
        group by customer_id
    ),
    first_last_order_date as (
            SELECT customer_id,
                    date_trunc('day',max(order_datetime)) last_order_date,
                    date_trunc('day',min(order_datetime)) first_order_date,
            FROM {{ ref('model_dm_order_mapping_customer_mapping_campaign') }}
            group by customer_id
            ),

    repeat_order as (
            select 
                    campaign_id,
                    count(distinct customer_id) repeat_customer_count
            from (  
                    SELECT campaign_id,
                            customer_id
                    FROM {{ ref('model_dm_order_mapping_customer_mapping_campaign') }} 
                    where campaign_id is not null
                    group by campaign_id,customer_id
                    having count(*) > 1)
        group by campaign_id
    ),
    agg as (
        SELECT dm.campaign_id,c.channel,platform,
                sum(case when customer_order_sequence  = 1 then 1 else 0 end) as new_customer_count,
                sum(case when customer_order_sequence_after_validate  = 1 then 1 else 0 end) as first_orders_from_validate_customers_count,
                cast(avg(total_amount) as decimal(10,2)) avg_order_value,
                count(distinct order_id) transaction_count,
                cast(avg(date_diff('day',flo.first_order_date,greatest(flo.last_order_date,s.last_session_date))) as decimal(10,2)) avg_days_active_after_first_purchase,
                cast(avg(date_diff('day',least(flo.first_order_date,s.first_session_date),flo.first_order_date)) as decimal(10,2)) avg_days_from_first_interaction_to_purchase,
                sum(ad_cost) ad_cost
                                    
                
        FROM {{ ref('model_dm_order_mapping_customer_mapping_campaign') }} dm
        left join first_last_session_start s on dm.customer_id = s.customer_id
        left join first_last_order_date flo on dm.customer_id = flo.customer_id
        left join {{ ref('model_dim_campaign') }} c on c.campaign_id = dm.campaign_id
        where dm.campaign_id is not null
        group by dm.campaign_id,c.channel,platform
        )

select a.*,
        repeat_customer_count ,
        cast(ad_cost/nullif(new_customer_count,0) as decimal(10,2)) cost_of_customer_acquisiton,
        current_timestamp as report_load_dt
from agg a
left join repeat_order r on a.campaign_id = r.campaign_id


