with expected as (
 select cast(created_time as date) as observation_date,source_channel,currency,count(*) as observation_count
 from {{ ref('silver_ecommerce_observations') }} group by 1,2,3
), actual as (
{% for name in ['gold_observation_quality','gold_channel_daily','gold_status_distribution'] %}
{% if not loop.first %}union all{% endif %}
 select '{{ name }}' as model_name,observation_date,source_channel,currency,sum(observation_count) as observation_count
 from {{ ref(name) }} group by 1,2,3,4
{% endfor %}
), expected_models as (
{% for name in ['gold_observation_quality','gold_channel_daily','gold_status_distribution'] %}
{% if not loop.first %}union all{% endif %}
 select '{{ name }}' as model_name,* from expected
{% endfor %}
)
select * from (
 (select * from expected_models except all select * from actual)
 union all (select * from actual except all select * from expected_models)
)
