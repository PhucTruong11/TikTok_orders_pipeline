{{ config(tags=['gold', 'observations', 'data_quality']) }}
select source_channel, currency, _file_sha256 as source_file_sha256,
    min(cast(created_time as date)) as first_observation_date,
    max(cast(created_time as date)) as last_observation_date,
    count(*) as observation_count, max(_ingested_at) as last_build_at
from {{ ref('silver_ecommerce_observations') }} group by 1,2,3
