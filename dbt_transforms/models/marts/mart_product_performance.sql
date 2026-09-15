-- MART: Hiệu suất sản phẩm (Product Performance)
-- Top SKU bán chạy, doanh thu theo sản phẩm, tỷ lệ hủy theo sản phẩm

select
    product_id,
    product_name,
    sku_id,
    sku_name,
    seller_sku,

    -- === Số lượng ===
    count(distinct order_id) as total_orders,
    sum(quantity) as total_qty_sold,

    -- === Doanh thu ===
    sum(item_revenue) as total_revenue,
    sum(item_original_revenue) as total_original_revenue,
    sum(item_total_discount_amount) as total_discount_given,

    -- === Giá trung bình ===
    round(avg(item_sale_price), 0) as avg_sale_price,
    round(avg(item_original_price), 0) as avg_original_price,

    -- === Tỷ lệ giảm giá trung bình (%) ===
    round(
        (1 - sum(item_revenue) / nullif(sum(item_original_revenue), 0)) * 100,
        1
    ) as avg_discount_pct,

    -- === Phân tích hủy đơn theo sản phẩm ===
    count(case when order_status = 'CANCELLED' then 1 end) as cancelled_orders,
    round(
        count(case when order_status = 'CANCELLED' then 1 end) * 100.0
        / nullif(count(distinct order_id), 0),
        2
    ) as cancellation_rate_pct,

    -- === Lý do hủy phổ biến nhất ===
    mode(item_cancel_reason) filter (where item_cancel_reason is not null) as top_cancel_reason

from {{ ref('fct_order_items') }}
group by
    product_id,
    product_name,
    sku_id,
    sku_name,
    seller_sku
order by total_revenue desc
