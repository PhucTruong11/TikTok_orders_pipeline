-- Fact Orders: 1 dòng = 1 đơn hàng duy nhất
-- Tách các cột ORDER-LEVEL ra khỏi bảng phẳng, tránh bẫy nhân đôi doanh thu
-- Dùng row_number để lấy 1 dòng đại diện cho mỗi order_id

with items_per_order as (
    select
        order_id,
        count(*) as total_items,
        sum(quantity) as total_quantity,
        sum(item_sale_price * quantity) as calculated_item_revenue,
        sum(item_platform_disc) as total_item_platform_disc,
        sum(item_seller_disc) as total_item_seller_disc
    from {{ ref('int_orders_deduped') }}
    group by order_id
),

order_header as (
    select
        *,
        row_number() over (
            partition by order_id
            order by row_idx asc
        ) as _order_rank
    from {{ ref('int_orders_deduped') }}
)

select
    -- === Khóa ===
    h.order_id,
    h.source_channel,

    -- === Trạng thái & Loại đơn ===
    h.order_status,
    h.order_type,
    h.shop_name,

    -- === Mốc thời gian ===
    h.created_time,
    h.paid_time,
    h.rts_time,
    h.shipped_time,
    h.delivered_time,
    h.completed_time,
    h.cancelled_time,
    h.updated_time,

    -- === SLA ===
    h.rts_sla_time,
    h.tts_sla_time,
    h.delivery_sla_time,
    h.cancel_sla_time,
    h.auto_cancel_time,

    -- === Tài chính cấp đơn hàng (ORDER LEVEL - KHÔNG bị nhân đôi) ===
    h.currency,
    h.total_amount,
    h.sub_total,
    h.order_original_price,
    h.payment_method,
    h.is_cod,

    -- === Phí vận chuyển ===
    h.shipping_fee,
    h.original_shipping_fee,

    -- === Thuế & Phí phụ ===
    h.tax_amount,
    h.product_tax,
    h.shipping_tax,
    h.small_order_fee,
    h.retail_delivery_fee,
    h.insurance_fee,

    -- === Chiết khấu cấp đơn ===
    h.seller_discount,
    h.platform_discount,
    h.shipping_seller_discount,
    h.shipping_platform_discount,

    -- === Vận chuyển ===
    h.fulfillment_type,
    h.delivery_type,
    h.delivery_option,
    h.shipping_provider,
    h.tracking_number,
    h.warehouse_id,
    h.package_id,
    h.package_status,

    -- === Người mua & Địa chỉ giao ===
    h.buyer_uid,
    h.buyer_name,
    h.buyer_message,
    h.recipient_name,
    h.recipient_phone,
    h.full_address,
    h.postal_code,
    h.region_state,
    h.city_town,
    h.district,

    -- === Thống kê từ line items ===
    i.total_items,
    i.total_quantity,
    i.calculated_item_revenue,
    i.total_item_platform_disc,
    i.total_item_seller_disc,

    -- === Chỉ số tài chính tính toán ===
    h.total_amount
        - h.shipping_fee
        - h.tax_amount
        - h.small_order_fee
        - h.retail_delivery_fee
        - h.insurance_fee
        as net_revenue,

    h.seller_discount + h.platform_discount
        as total_discount,

    h.shipping_seller_discount + h.shipping_platform_discount
        as total_shipping_discount,

    -- === Metadata ===
    h.synced_created_at,
    h.synced_updated_at

from order_header h
inner join items_per_order i
    on h.order_id = i.order_id
where h._order_rank = 1
