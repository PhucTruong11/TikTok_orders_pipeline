-- Preserve every CSV record through Bronze/Silver, including masked-key collisions.
with violations as (
{% for ch in ['tiktok','shopee'] %}
    {% if not loop.first %}union all{% endif %}
    select '{{ ch }}_source_count' as check_name
    where (select count(*) from {{ source('raw_exports', ch ~ '_orders') }})
        <> (select count(*) from {{ ref('bronze_' ~ ch ~ '_orders') }})
    union all
    select '{{ ch }}_silver_keys' as check_name
    where exists (
        select observation_id from {{ ref('bronze_' ~ ch ~ '_orders') }}
        except all select observation_id from {{ ref('silver_' ~ ch ~ '_observations') }}
    ) or exists (
        select observation_id from {{ ref('silver_' ~ ch ~ '_observations') }}
        except all select observation_id from {{ ref('bronze_' ~ ch ~ '_orders') }}
    )
    union all
    select '{{ ch }}_union_keys' as check_name
    where exists (
        select observation_id from {{ ref('silver_' ~ ch ~ '_observations') }}
        except all select observation_id from {{ ref('silver_ecommerce_observations') }} where source_channel='{{ ch }}'
    ) or exists (
        select observation_id from {{ ref('silver_ecommerce_observations') }} where source_channel='{{ ch }}'
        except all select observation_id from {{ ref('silver_' ~ ch ~ '_observations') }}
    )
{% endfor %}
)
select * from violations
