{% macro normalize_order_status(column_name) -%}
    case upper(trim({{ column_name }}))
        when 'CANCELED' then 'CANCELLED'
        else upper(trim({{ column_name }}))
    end
{%- endmacro %}
