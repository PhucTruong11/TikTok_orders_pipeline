-- MART: Doanh thu theo ngày
-- Dùng để theo dõi xu hướng bán hàng, so sánh ngày/tuần/tháng

select
    cast(created_time as date) as order_date,

    -- === Số lượng đơn ===
    count(*) as total_orders,
    count(case when order_status = 'COMPLETED' then 1 end) as completed_orders,
    count(case when order_status = 'CANCELLED' then 1 end) as cancelled_orders,
    count(case when order_status not in ('COMPLETED', 'CANCELLED') then 1 end) as processing_orders,

    -- === Tỷ lệ ===
    round(
        count(case when order_status = 'COMPLETED' then 1 end) * 100.0 / nullif(count(*), 0),
        2
    ) as completion_rate_pct,
    round(
        count(case when order_status = 'CANCELLED' then 1 end) * 100.0 / nullif(count(*), 0),
        2
    ) as cancellation_rate_pct,

    -- === Doanh thu (chỉ tính đơn COMPLETED) ===
    sum(case when order_status = 'COMPLETED' then total_amount else 0 end) as gross_revenue,
    sum(case when order_status = 'COMPLETED' then net_revenue else 0 end) as net_revenue,
    sum(case when order_status = 'COMPLETED' then total_discount else 0 end) as total_discount,
    sum(case when order_status = 'COMPLETED' then shipping_fee else 0 end) as total_shipping_fee,
    sum(case when order_status = 'COMPLETED' then platform_discount else 0 end) as total_platform_discount,
    sum(case when order_status = 'COMPLETED' then seller_discount else 0 end) as total_seller_discount,

    -- === Đơn hàng trung bình (AOV) ===
    round(
        sum(case when order_status = 'COMPLETED' then total_amount else 0 end)
        / nullif(count(case when order_status = 'COMPLETED' then 1 end), 0),
        0
    ) as avg_order_value,

    -- === Số lượng sản phẩm ===
    sum(case when order_status = 'COMPLETED' then total_quantity else 0 end) as total_items_sold

from {{ ref('fct_orders') }}
group by cast(created_time as date)
order by order_date desc
