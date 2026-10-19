-- Compatibility view: one CSV observation, not a verified order line.
-- Join on observation_id, never masked order_id/pk_id.
{{ config(tags=['tiktok', 'observations', 'compatibility']) }}

select * from {{ ref('fct_tiktok_observations') }}
