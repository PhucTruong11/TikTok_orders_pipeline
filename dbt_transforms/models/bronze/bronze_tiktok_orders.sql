{{ config(tags=['bronze', 'tiktok', 'observations']) }}
-- Raw string columns are preserved. Added columns identify a snapshot record only.
{% set input_path = env_var('TIKTOK_CSV_PATH', '../data/raw/tiktok_orders.csv') | replace("'", "''") %}
with records as (
    select row_number() over () as _source_row_number, *
    from {{ source('raw_exports', 'tiktok_orders') }}
), fingerprint as (
    select sha256(content) as _file_sha256 from read_blob('{{ input_path }}')
)
select r.*,
    'tiktok' as source_channel,
    f._file_sha256,
    '{{ input_path }}' as _source_file,
    current_timestamp as _ingested_at,
    sha256('tiktok|' || f._file_sha256 || '|' || cast(r._source_row_number as varchar)) as observation_id
from records r cross join fingerprint f
