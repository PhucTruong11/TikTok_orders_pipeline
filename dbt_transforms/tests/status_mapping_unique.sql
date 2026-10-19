select channel, raw_status
from {{ ref('order_status_mapping') }}
group by 1, 2
having count(*) <> 1
