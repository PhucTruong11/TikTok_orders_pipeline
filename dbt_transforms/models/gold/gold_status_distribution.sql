{{ config(tags=['gold', 'observations', 'ecommerce']) }}
select cast(created_time as date) as observation_date, source_channel, currency,
    standardized_status, status_group, count(*) as observation_count
from {{ ref('silver_ecommerce_observations') }} group by 1,2,3,4,5
