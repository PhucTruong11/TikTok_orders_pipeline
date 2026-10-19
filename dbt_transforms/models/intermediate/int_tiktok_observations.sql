{{ config(tags=['tiktok', 'observations']) }}

select
    s.*,
    m.standardized_status,
    m.status_group,
    m.raw_status is not null as is_status_mapped,
    case when m.status_group = 'COMPLETED' then 1 else 0 end as is_completed,
    case when m.status_group = 'CANCELLED' then 1 else 0 end as is_cancelled,
    case when m.status_group = 'RETURNED' then 1 else 0 end as is_returned
from {{ ref('stg_tiktok_orders') }} s
left join {{ ref('order_status_mapping') }} m
    on s.source_channel = m.channel
    and s.order_status = m.raw_status
