-- Khử trùng (Deduplication): Loại bỏ 4,689 dòng pk_id trùng lặp
-- Giữ lại bản ghi có synced_updated_at MỚI NHẤT (dữ liệu đồng bộ gần nhất)

with ranked as (
    select
        *,
        row_number() over (
            partition by pk_id
            order by synced_updated_at desc
        ) as _row_rank
    from {{ ref('stg_tiktok_orders') }}
)

select
    * exclude (_row_rank)
from ranked
where _row_rank = 1
