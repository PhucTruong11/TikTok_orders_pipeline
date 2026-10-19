-- Compatibility name retained for lineage. Every observation is preserved.
-- Masked pk_id/order_id are labels; no dedup or global timestamp watermark.
{{ config(materialized='view', tags=['tiktok', 'observations', 'compatibility']) }}

select * from {{ ref('int_tiktok_observations') }}
