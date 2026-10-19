{{ config(tags=['gold', 'observations', 'ecommerce']) }}
-- Sums describe observations in the supplied snapshots, not verified order revenue.
select cast(created_time as date) as observation_date, source_channel, currency,
    count(*) as observation_count,
    cast(sum(quantity) as bigint) as observed_unit_count,
    cast(sum(case when item_sale_price is not null then quantity else 0 end) as bigint) as priced_unit_count,
    count(*) filter(where item_sale_price is null) as missing_sale_price_count,
    sum(observed_item_sales_value) as observed_sales_value_sum,
    sum(observed_item_sales_value) / nullif(sum(case when item_sale_price is not null then quantity else 0 end),0) as weighted_observed_sale_price,
    min(item_sale_price) as minimum_observed_sale_price,
    max(item_sale_price) as maximum_observed_sale_price
from {{ ref('silver_ecommerce_observations') }} group by 1,2,3
