{{
    config(
        materialized='incremental',
        unique_key='pk_id'
    )
}}

-- Khử trùng (Deduplication) và Incremental Load
-- Giữ lại bản ghi có synced_updated_at MỚI NHẤT

with new_data as (
    select *
    from {{ ref('stg_tiktok_orders') }}
    
    {% if is_incremental() %}
        -- Watermark: Chỉ lấy các bản ghi có thời gian đồng bộ mới hơn hoặc bằng dữ liệu hiện có
        -- Điều này giúp bắt được dữ liệu mới (new) và dữ liệu cập nhật trễ (late-arriving data)
        where synced_updated_at >= (select coalesce(max(synced_updated_at), '1970-01-01') from {{ this }})
    {% endif %}
)

-- Sử dụng QUALIFY để loại bỏ trùng lặp NỘI BỘ trong mẻ dữ liệu mới
select *
from new_data
qualify row_number() over (
    partition by pk_id
    order by synced_updated_at desc, row_idx desc
) = 1
