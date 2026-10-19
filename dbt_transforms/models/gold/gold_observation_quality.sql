{{ config(tags=['gold', 'observations', 'data_quality']) }}
select cast(created_time as date) as observation_date, source_channel, currency,
    count(*) as observation_count,
    count(*) filter(where is_completed=1) as completed_observation_count,
    count(*) filter(where is_cancelled=1) as cancelled_observation_count,
    count(*) filter(where is_returned=1) as returned_observation_count,
    count(*) filter(where is_return_requested=1) as return_requested_observation_count,
    count(*) filter(where not is_status_mapped) as unmapped_status_count,
    count(*) filter(where is_pk_masked) as masked_pk_count,
    count(*) filter(where is_order_id_masked) as masked_order_id_count,
    count(*) filter(where item_sale_price is null) as missing_sale_price_count,
    count(*) filter(where shipped_time is not null) as shipped_time_populated_count,
    count(*) filter(where completed_time is not null) as completed_time_populated_count,
    count(*) filter(where delivered_time is not null and delivery_sla_time is not null) as delivery_sla_eligible_count,
    count(*) filter(where delivered_time is not null and delivery_sla_time is not null and delivered_time<=delivery_sla_time) as delivery_on_time_observation_count,
    count(*) filter(where delivered_time is not null and delivery_sla_time is not null and delivered_time>delivery_sla_time) as delivery_late_observation_count
from {{ ref('silver_ecommerce_observations') }} group by 1,2,3
