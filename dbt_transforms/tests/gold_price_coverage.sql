select * from {{ ref('gold_channel_daily') }}
where priced_unit_count>observed_unit_count or missing_sale_price_count>observation_count
 or (priced_unit_count=0 and weighted_observed_sale_price is not null)
 or (priced_unit_count>0 and weighted_observed_sale_price is null)
 or abs(weighted_observed_sale_price*priced_unit_count-observed_sales_value_sum)>0.01
