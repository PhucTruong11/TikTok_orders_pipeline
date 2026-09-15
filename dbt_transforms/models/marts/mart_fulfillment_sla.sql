-- MART: Phân tích SLA vận hành (Fulfillment & Delivery)
-- Theo dõi: đóng gói có kịp SLA không? Giao hàng có trễ không?

select
    cast(created_time as date) as order_date,
    shipping_provider,
    region_state,

    count(*) as total_orders,

    -- === SLA Đóng gói (Ready-to-Ship) ===
    -- So sánh rts_time vs rts_sla_time: nếu rts_time <= rts_sla_time thì đạt SLA
    count(case
        when rts_time is not null and rts_sla_time is not null
            and rts_time <= rts_sla_time
        then 1
    end) as rts_on_time,

    count(case
        when rts_time is not null and rts_sla_time is not null
            and rts_time > rts_sla_time
        then 1
    end) as rts_late,

    round(
        count(case
            when rts_time is not null and rts_sla_time is not null
                and rts_time <= rts_sla_time
            then 1
        end) * 100.0
        / nullif(count(case when rts_time is not null and rts_sla_time is not null then 1 end), 0),
        2
    ) as rts_on_time_pct,

    -- === SLA Giao hàng (Delivery) ===
    count(case
        when delivered_time is not null and delivery_sla_time is not null
            and delivered_time <= delivery_sla_time
        then 1
    end) as delivery_on_time,

    count(case
        when delivered_time is not null and delivery_sla_time is not null
            and delivered_time > delivery_sla_time
        then 1
    end) as delivery_late,

    round(
        count(case
            when delivered_time is not null and delivery_sla_time is not null
                and delivered_time <= delivery_sla_time
            then 1
        end) * 100.0
        / nullif(count(case when delivered_time is not null and delivery_sla_time is not null then 1 end), 0),
        2
    ) as delivery_on_time_pct,

    -- === Thời gian xử lý trung bình (giờ) ===
    round(
        avg(case
            when rts_time is not null and paid_time is not null
            then extract(epoch from (rts_time - paid_time)) / 3600.0
        end),
        1
    ) as avg_processing_hours,

    round(
        avg(case
            when delivered_time is not null and rts_time is not null
            then extract(epoch from (delivered_time - rts_time)) / 3600.0
        end),
        1
    ) as avg_delivery_hours

from {{ ref('fct_orders') }}
where order_status not in ('UNPAID')
group by
    cast(created_time as date),
    shipping_provider,
    region_state
order by order_date desc
