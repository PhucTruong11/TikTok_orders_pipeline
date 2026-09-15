-- Fact Order Items: 1 dòng = 1 sản phẩm (SKU) trong đơn hàng
-- Chỉ giữ các cột ITEM-LEVEL, tham chiếu sang fct_orders qua order_id

select
    -- === Khóa ===
    pk_id,
    order_id,
    source_channel,

    -- === Chi tiết sản phẩm ===
    product_id,
    product_name,
    sku_id,
    sku_name,
    seller_sku,
    sku_image,

    -- === Số lượng & Giá ===
    quantity,
    item_sale_price,
    item_original_price,
    item_platform_disc,
    item_seller_disc,

    -- === Tính toán ===
    item_sale_price * quantity as item_revenue,
    item_original_price * quantity as item_original_revenue,
    (item_original_price - item_sale_price) * quantity as item_total_discount_amount,

    -- === Trạng thái item ===
    item_status,
    item_cancel_reason,
    row_idx,

    -- === Thời gian đơn (để phân tích item theo thời gian) ===
    created_time,
    order_status

from {{ ref('int_orders_deduped') }}
