with expected as (
    select cast(created_time as date) as observation_date, source_channel, currency,
        count(*) as observation_count
    from {{ ref('fct_tiktok_observations') }} group by 1, 2, 3
), actual as (
    select observation_date, source_channel, currency, observation_count
    from {{ ref('mart_tiktok_observation_quality') }}
)
select * from (
    (select * from expected except all select * from actual)
    union all
    (select * from actual except all select * from expected)
)
