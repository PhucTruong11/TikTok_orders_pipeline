{% for name in ['gold_observation_quality','gold_channel_daily'] %}
{% if not loop.first %}union all{% endif %}
select '{{ name }}' as model_name from {{ ref(name) }}
group by observation_date,source_channel,currency having count(*)<>1
{% endfor %}
union all
select 'gold_status_distribution' as model_name from {{ ref('gold_status_distribution') }}
group by observation_date,source_channel,currency,standardized_status,status_group having count(*)<>1
union all
select 'gold_pipeline_metadata' as model_name from {{ ref('gold_pipeline_metadata') }}
group by source_channel,currency,source_file_sha256 having count(*)<>1
