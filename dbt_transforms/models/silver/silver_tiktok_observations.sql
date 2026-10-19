{{ config(tags=['silver', 'tiktok', 'observations']) }}

with typed as (
select
    observation_id, source_channel, _source_row_number, _file_sha256,
    _source_file, _ingested_at,
    contains(cast(pkId as varchar), '*') as is_pk_masked,
    contains(cast(order_id as varchar), '*') as is_order_id_masked,
    -- === Metadata hệ thống ===
    cast(pkId as varchar)               as pk_id,
    cast(user_id as varchar)            as user_id,
    cast(createdAt as timestamp)        as synced_created_at,
    cast(updatedAt as timestamp)        as synced_updated_at,

    -- === Thông tin đơn hàng ===
    cast(order_id as varchar)           as order_id,
    cast(order_status as varchar)       as raw_order_status,
    {{ normalize_order_status('order_status') }} as order_status,
    cast(order_type as varchar)         as order_type,
    {{ clean_string('shop_name') }}     as shop_name,

    -- === Mốc thời gian đơn hàng ===
    cast(created_time as timestamp)     as created_time,
    cast(paid_time as timestamp)        as paid_time,
    cast(rts_time as timestamp)         as rts_time,
    cast(shipped_time as timestamp)     as shipped_time,
    cast(delivered_time as timestamp)   as delivered_time,
    cast(completed_time as timestamp)   as completed_time,
    cast(cancelled_time as timestamp)   as cancelled_time,
    cast(updated_time as timestamp)     as updated_time,

    -- === SLA (Service Level Agreement) ===
    cast(rts_sla_time as timestamp)     as rts_sla_time,
    cast(tts_sla_time as timestamp)     as tts_sla_time,
    cast(delivery_sla_time as timestamp) as delivery_sla_time,
    cast(cancel_sla_time as timestamp)  as cancel_sla_time,
    cast(auto_cancel_time as timestamp) as auto_cancel_time,

    -- === Tài chính - Cấp đơn hàng (ORDER LEVEL) ===
    upper(trim(cast(currency as varchar)))      as currency,
    cast(total_amount as decimal(18,2))         as total_amount,
    cast(sub_total as decimal(18,2))            as sub_total,
    cast(order_original_price as decimal(18,2)) as order_original_price,
    cast(payment_method as varchar)             as payment_method,
    cast(is_cod as varchar)                     as is_cod,

    -- === Phí vận chuyển ===
    cast(shipping_fee as decimal(18,2))             as shipping_fee,
    cast(original_shipping_fee as decimal(18,2))    as original_shipping_fee,

    -- === Thuế & Phí phụ ===
    cast(tax_amount as decimal(18,2))           as tax_amount,
    cast(product_tax as decimal(18,2))          as product_tax,
    cast(shipping_tax as decimal(18,2))         as shipping_tax,
    cast(small_order_fee as decimal(18,2))      as small_order_fee,
    cast(retail_delivery_fee as decimal(18,2))  as retail_delivery_fee,
    cast(insurance_fee as decimal(18,2))        as insurance_fee,

    -- === Chiết khấu ===
    cast(seller_discount as decimal(18,2))              as seller_discount,
    cast(platform_discount as decimal(18,2))            as platform_discount,
    cast(shipping_seller_discount as decimal(18,2))     as shipping_seller_discount,
    cast(shipping_platform_discount as decimal(18,2))   as shipping_platform_discount,

    -- === Vận chuyển & Giao hàng ===
    cast(fulfillment_type as varchar)   as fulfillment_type,
    cast(delivery_type as varchar)      as delivery_type,
    cast(delivery_option as varchar)    as delivery_option,
    cast(shipping_provider as varchar)  as shipping_provider,
    cast(tracking_number as varchar)    as tracking_number,
    cast(warehouse_id as varchar)       as warehouse_id,

    -- === Thông tin người mua ===
    cast(buyer_uid as varchar)          as buyer_uid,
    {{ clean_string('buyer_name') }}    as buyer_name,
    {{ clean_string('buyer_message') }} as buyer_message,

    -- === Địa chỉ giao hàng ===
    {{ clean_string('recipient_name') }} as recipient_name,
    cast(recipient_phone as varchar)    as recipient_phone,
    {{ clean_string('full_address') }}  as full_address,
    cast(postal_code as varchar)        as postal_code,
    cast(region_state as varchar)       as region_state,
    cast(city_town as varchar)          as city_town,
    cast(district as varchar)           as district,

    -- === Chi tiết sản phẩm - Cấp LINE ITEM ===
    {{ clean_string('product_name') }}          as product_name,
    cast(sku_name as varchar)                   as sku_name,
    cast(seller_sku as varchar)                 as seller_sku,
    cast(quantity as integer)                   as quantity,
    cast(item_status as varchar)                as item_status,
    cast(item_sale_price as decimal(18,2))      as item_sale_price,
    cast(item_original_price as decimal(18,2))  as item_original_price,
    cast(item_platform_disc as decimal(18,2))   as item_platform_disc,
    cast(item_seller_disc as decimal(18,2))     as item_seller_disc,
    cast(product_id as varchar)                 as product_id,
    cast(sku_id as varchar)                     as sku_id,
    cast(package_id as varchar)                 as package_id,
    cast(package_status as varchar)             as package_status,
    cast(item_cancel_reason as varchar)         as item_cancel_reason,
    cast(sku_image as varchar)                  as sku_image,
    cast(row_idx as integer)                    as row_idx
from {{ ref('bronze_tiktok_orders') }}
), mapped as (
select t.*,
    m.standardized_status, m.status_group,
    m.raw_status is not null as is_status_mapped,
    case when m.status_group = 'COMPLETED' then 1 else 0 end as is_completed,
    case when m.status_group = 'CANCELLED' then 1 else 0 end as is_cancelled,
    case when m.status_group = 'RETURNED' then 1 else 0 end as is_returned,
    case when m.status_group = 'RETURN_REQUESTED' then 1 else 0 end as is_return_requested
from typed t
left join {{ ref('silver_order_status_mapping') }} m
    on t.source_channel = m.channel and t.order_status = m.raw_status
)
select mapped.*,
    item_sale_price * quantity as observed_item_sales_value,
    item_original_price * quantity as observed_item_original_value,
    (item_original_price - item_sale_price) * quantity as observed_item_price_difference,
    total_amount is not null and shipping_fee is not null and tax_amount is not null
        and small_order_fee is not null and retail_delivery_fee is not null
        and insurance_fee is not null as has_complete_amount_components,
    total_amount - shipping_fee - tax_amount - small_order_fee - retail_delivery_fee
        - insurance_fee as amount_excluding_reported_shipping_tax_and_fees
from mapped
