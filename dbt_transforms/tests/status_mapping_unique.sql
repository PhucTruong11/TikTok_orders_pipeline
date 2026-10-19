select channel, raw_status from {{ ref('silver_order_status_mapping') }} group by 1,2 having count(*) <> 1
