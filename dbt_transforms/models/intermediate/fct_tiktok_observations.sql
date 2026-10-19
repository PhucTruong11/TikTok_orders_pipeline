{{ config(tags=['tiktok', 'observations']) }}

-- One observed CSV record, not a verified order or SKU.
-- Header money is preserved for inspection, never summed as order revenue.
select
    o.*,
    item_sale_price * quantity as observed_item_sales_value,
    item_original_price * quantity as observed_item_original_value,
    (item_original_price - item_sale_price) * quantity as observed_item_price_difference,
    total_amount is not null and shipping_fee is not null and tax_amount is not null
        and small_order_fee is not null and retail_delivery_fee is not null
        and insurance_fee is not null as has_complete_amount_components,
    total_amount - shipping_fee - tax_amount - small_order_fee
        - retail_delivery_fee - insurance_fee as amount_excluding_reported_shipping_tax_and_fees
from {{ ref('int_orders_deduped') }} o
