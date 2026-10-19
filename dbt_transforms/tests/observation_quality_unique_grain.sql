select observation_date, source_channel, currency
from {{ ref('mart_tiktok_observation_quality') }}
group by 1, 2, 3 having count(*) <> 1
