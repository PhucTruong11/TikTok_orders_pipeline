select * from {{ ref('gold_observation_quality') }}
where completed_observation_count+cancelled_observation_count+returned_observation_count+return_requested_observation_count > observation_count
 or delivery_on_time_observation_count+delivery_late_observation_count <> delivery_sla_eligible_count
{% for field in ['masked_pk_count','masked_order_id_count','missing_sale_price_count','shipped_time_populated_count','completed_time_populated_count','delivery_sla_eligible_count'] %}
 or {{ field }} > observation_count or {{ field }} < 0
{% endfor %}
 or unmapped_status_count > 0
