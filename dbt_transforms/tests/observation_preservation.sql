-- No join fan-out, missing records, or masked-key dedup across any active layer.
{% set downstream = ['int_tiktok_observations', 'int_orders_deduped', 'fct_tiktok_observations', 'fct_order_items'] %}
with stage as (
    select observation_id from {{ ref('stg_tiktok_orders') }}
),
violations as (
    select 'source_record_count' as check_name
    where (select count(*) from {{ source('raw_tiktok', 'tiktok_orders') }})
        <> (select count(*) from stage)
    {% for name in downstream %}
    union all
    select '{{ name }}' as check_name
    where exists (
        select observation_id from stage
        except all select observation_id from {{ ref(name) }}
    ) or exists (
        select observation_id from {{ ref(name) }}
        except all select observation_id from stage
    )
    {% endfor %}
)
select * from violations
