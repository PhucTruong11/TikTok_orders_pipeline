select * from {{ ref('mart_tiktok_observation_quality') }}
where completed_observation_count + cancelled_observation_count + returned_observation_count > observation_count
    or masked_pk_count > observation_count
    or masked_order_id_count > observation_count
    or delivery_sla_eligible_count > observation_count
    or delivery_on_time_observation_count + delivery_late_observation_count <> delivery_sla_eligible_count
    or shipped_time_populated_count > observation_count
    or completed_time_populated_count > observation_count
    or incomplete_amount_components_count > observation_count
    or unmapped_status_count > 0
