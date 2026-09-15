-- MART: Phân tích theo Khu vực Địa lý
-- Doanh thu, số đơn, tỷ lệ hủy theo Tỉnh/Thành phố & Quận/Huyện

select
    coalesce(region_state, 'Không xác định') as region_state,
    coalesce(city_town, 'Không xác định') as city_town,
    coalesce(district, 'Không xác định') as district,

    -- === Số lượng đơn ===
    count(*) as total_orders,
    count(case when order_status = 'COMPLETED' then 1 end) as completed_orders,
    count(case when order_status = 'CANCELLED' then 1 end) as cancelled_orders,

    -- === Tỷ lệ ===
    round(
        count(case when order_status = 'COMPLETED' then 1 end) * 100.0 / nullif(count(*), 0),
        2
    ) as completion_rate_pct,
    round(
        count(case when order_status = 'CANCELLED' then 1 end) * 100.0 / nullif(count(*), 0),
        2
    ) as cancellation_rate_pct,

    -- === Doanh thu (đơn COMPLETED) ===
    sum(case when order_status = 'COMPLETED' then total_amount else 0 end) as gross_revenue,
    sum(case when order_status = 'COMPLETED' then net_revenue else 0 end) as net_revenue,

    -- === AOV theo khu vực ===
    round(
        sum(case when order_status = 'COMPLETED' then total_amount else 0 end)
        / nullif(count(case when order_status = 'COMPLETED' then 1 end), 0),
        0
    ) as avg_order_value,

    -- === Vận chuyển ===
    sum(case when order_status = 'COMPLETED' then shipping_fee else 0 end) as total_shipping_cost,

    -- === Phương thức thanh toán phổ biến ===
    mode(payment_method) as top_payment_method,

    -- === COD ratio ===
    round(
        count(case when is_cod = 'true' then 1 end) * 100.0 / nullif(count(*), 0),
        2
    ) as cod_rate_pct,

    -- === SLA giao hàng theo khu vực ===
    round(
        avg(case
            when delivered_time is not null and rts_time is not null
            then extract(epoch from (delivered_time - rts_time)) / 3600.0
        end),
        1
    ) as avg_delivery_hours

from {{ ref('fct_orders') }}
group by
    coalesce(region_state, 'Không xác định'),
    coalesce(city_town, 'Không xác định'),
    coalesce(district, 'Không xác định')
order by gross_revenue desc
