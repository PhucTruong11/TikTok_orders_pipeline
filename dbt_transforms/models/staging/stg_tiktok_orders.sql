-- Model staging: Ép kiểu dữ liệu + đặt alias chuẩn cho 71 cột TikTok
-- Grain (hạt độ): 1 dòng = 1 line item (sản phẩm) trong đơn hàng
-- Đọc trực tiếp file CSV thô qua hàm read_csv_auto() của DuckDB

with source as (
    select * from read_csv_auto('../data/raw/tiktok_orders.csv')
)

select
    -- === Metadata hệ thống ===
    cast(pkId as varchar)               as pk_id,
    cast(user_id as varchar)            as user_id,
    cast(createdAt as timestamp)        as synced_created_at,
    cast(updatedAt as timestamp)        as synced_updated_at,

    -- === Thông tin đơn hàng ===
    cast(order_id as varchar)           as order_id,
    cast(order_status as varchar)       as order_status,
    cast(order_type as varchar)         as order_type,
    cast(shop_name as varchar)          as shop_name,

    -- === Mốc thời gian đơn hàng ===
    cast(created_time as timestamp)     as created_time,
    cast(paid_time as timestamp)        as paid_time,
    cast(rts_time as timestamp)         as rts_time,
    cast(shipped_time as varchar)       as shipped_time,
    cast(delivered_time as timestamp)   as delivered_time,
    cast(completed_time as varchar)     as completed_time,
    cast(cancelled_time as timestamp)   as cancelled_time,
    cast(updated_time as timestamp)     as updated_time,

    -- === SLA (Service Level Agreement) ===
    cast(rts_sla_time as timestamp)     as rts_sla_time,
    cast(tts_sla_time as timestamp)     as tts_sla_time,
    cast(delivery_sla_time as timestamp) as delivery_sla_time,
    cast(cancel_sla_time as timestamp)  as cancel_sla_time,
    cast(auto_cancel_time as varchar)   as auto_cancel_time,

    -- === Tài chính - Cấp đơn hàng (ORDER LEVEL) ===
    cast(currency as varchar)                   as currency,
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
    cast(buyer_name as varchar)         as buyer_name,
    cast(buyer_message as varchar)      as buyer_message,

    -- === Địa chỉ giao hàng ===
    cast(recipient_name as varchar)     as recipient_name,
    cast(recipient_phone as varchar)    as recipient_phone,
    cast(full_address as varchar)       as full_address,
    cast(postal_code as varchar)        as postal_code,
    cast(region_state as varchar)       as region_state,
    cast(city_town as varchar)          as city_town,
    cast(district as varchar)           as district,

    -- === Chi tiết sản phẩm - Cấp LINE ITEM ===
    cast(product_name as varchar)               as product_name,
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
    cast(row_idx as integer)                    as row_idx,

    -- === Cột bổ sung cho pipeline ===
    'tiktok' as source_channel

from source
