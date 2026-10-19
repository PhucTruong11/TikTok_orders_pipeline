{% for ch in ['tiktok','shopee'] %}
{% if not loop.first %}union all{% endif %}
select observation_id from {{ ref('bronze_' ~ ch ~ '_orders') }}
where try_cast(quantity as decimal(38,10)) is null
    or try_cast(quantity as decimal(38,10)) <> floor(try_cast(quantity as decimal(38,10)))
{% endfor %}
