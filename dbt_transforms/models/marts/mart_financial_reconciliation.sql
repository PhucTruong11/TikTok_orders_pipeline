-- MART: Đối soát Tài chính (Financial Reconciliation)
-- Tổng hợp dòng tiền: Doanh thu gộp → Chiết khấu → Phí → Doanh thu thực nhận
-- Theo tháng, để đối chiếu với báo cáo trên TikTok Seller Center

select
    date_trunc('month', created_time) as report_month,
    order_status,

    count(*) as total_orders,

    -- === 1. DOANH THU DANH NGHĨA (Gross GMV) ===
    -- Tổng giá trị đơn hàng trước mọi chiết khấu
    sum(order_original_price) as gross_gmv,

    -- === 2. CHIẾT KHẤU SÀN (Platform bears cost) ===
    sum(platform_discount) as platform_discount,
    sum(shipping_platform_discount) as shipping_platform_discount,

    -- === 3. CHIẾT KHẤU NHÀ BÁN (Seller bears cost) ===
    sum(seller_discount) as seller_discount,
    sum(shipping_seller_discount) as shipping_seller_discount,

    -- === 4. TỔNG CHIẾT KHẤU ===
    sum(total_discount) as total_discount,
    sum(total_shipping_discount) as total_shipping_discount,

    -- === 5. DOANH THU SAU CHIẾT KHẤU ===
    sum(total_amount) as total_amount_after_discount,
    sum(sub_total) as sub_total,

    -- === 6. PHÍ VẬN CHUYỂN ===
    sum(shipping_fee) as actual_shipping_fee,
    sum(original_shipping_fee) as original_shipping_fee,
    sum(original_shipping_fee) - sum(shipping_fee) as shipping_fee_subsidy,

    -- === 7. THUẾ & PHÍ PHỤ ===
    sum(tax_amount) as total_tax,
    sum(product_tax) as product_tax,
    sum(shipping_tax) as shipping_tax,
    sum(small_order_fee) as small_order_fee,
    sum(retail_delivery_fee) as retail_delivery_fee,
    sum(insurance_fee) as insurance_fee,

    -- === 8. DOANH THU THỰC NHẬN (Net Revenue) ===
    sum(net_revenue) as net_revenue,

    -- === 9. CHỈ SỐ PHÂN TÍCH ===
    -- Tỷ lệ chiết khấu trên GMV
    round(
        sum(total_discount) * 100.0 / nullif(sum(order_original_price), 0),
        2
    ) as discount_rate_pct,

    -- Tỷ lệ phí ship trên doanh thu
    round(
        sum(shipping_fee) * 100.0 / nullif(sum(total_amount), 0),
        2
    ) as shipping_cost_ratio_pct,

    -- Tỷ lệ thuế + phí phụ trên doanh thu
    round(
        (sum(tax_amount) + sum(small_order_fee) + sum(retail_delivery_fee) + sum(insurance_fee))
        * 100.0 / nullif(sum(total_amount), 0),
        2
    ) as tax_and_fees_ratio_pct,

    -- Margin sau tất cả phí (Net Revenue / GMV)
    round(
        sum(net_revenue) * 100.0 / nullif(sum(order_original_price), 0),
        2
    ) as net_margin_pct

from {{ ref('fct_orders') }}
group by
    date_trunc('month', created_time),
    order_status
order by report_month desc, gross_gmv desc
